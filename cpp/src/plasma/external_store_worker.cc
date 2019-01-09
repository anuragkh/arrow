#include <utility>

#include "external_store_worker.h"
#include "arrow/util/memory.h"

#define MAX_ENQUEUE 32
#define MEMCPY_NUM_THREADS 4
#define MEMCPY_BLOCK_SIZE 64
#define OBJECT_SIZE_THRESHOLD (1024 * 1024)

namespace plasma {

ExternalStoreWorker::ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                                         const std::string &store_socket)
    : external_store_(std::move(external_store)),
      client_(nullptr),
      store_socket_(store_socket),
      terminate_(false),
      stopped_(false) {
  if (external_store_) {
    worker_thread_ = std::thread(&ExternalStoreWorker::DoWork, this);
  }
}

ExternalStoreWorker::~ExternalStoreWorker() {
  if (!stopped_) {
    Shutdown();
  }
}

bool ExternalStoreWorker::IsValid() const {
  return external_store_ != nullptr;
}

Status ExternalStoreWorker::Get(const ObjectID &object_id,
                                std::string &object_data,
                                std::string &object_metadata) {
  std::vector<std::string> data, metadata;
  {
    std::unique_lock<std::mutex> lock(store_mutex_);
    ARROW_CHECK_OK(external_store_->Get({object_id}, &data, &metadata));
  }

  object_data = data.back();
  object_metadata = metadata.back();

  if (object_data.empty()) {
    return Status::PlasmaObjectNonexistent(object_id.binary());
  }
  return Status::OK();
}

Status ExternalStoreWorker::Get(const std::vector<ObjectID> &object_ids,
                                std::vector<std::string> *object_data,
                                std::vector<std::string> *object_metadata) {
  std::unique_lock<std::mutex> lock(store_mutex_);
  return external_store_->Get(object_ids, object_data, object_metadata);
}

Status ExternalStoreWorker::GetAndWriteToPlasma(const ObjectID &object_id) {
  size_t n_enqueued = 0;
  {
    std::unique_lock<std::mutex> lock(tasks_mutex_);
    if (object_ids_.size() > MAX_ENQUEUE) {
      return Status::CapacityError("Too many un-evict requests");
    }
    object_ids_.push_back(object_id);
    n_enqueued = object_ids_.size();
  }
  tasks_cv_.notify_one();
  ARROW_LOG(DEBUG) << "Enqueued " << n_enqueued << " requests";
  return Status::OK();
}

Status ExternalStoreWorker::Put(const std::vector<ObjectID> &object_ids,
                                const std::vector<std::string> &object_data,
                                const std::vector<std::string> &object_metadata) {
  std::unique_lock<std::mutex> lock(store_mutex_);
  return external_store_->Put(object_ids, object_data, object_metadata);
}

void ExternalStoreWorker::Shutdown() {
  {
    std::unique_lock<std::mutex> lock(tasks_mutex_);
    terminate_ = true;
  }

  tasks_cv_.notify_all();
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
  stopped_ = true;
}

void ExternalStoreWorker::DoWork() {
  while (true) {
    std::vector<ObjectID> object_ids;
    {
      std::unique_lock<std::mutex> lock(tasks_mutex_);

      // Wait for ObjectIds to become available
      tasks_cv_.wait(lock, [this] {
        return !object_ids_.empty() || terminate_;
      });

      // Stop execution if termination signal has been set and there are no
      // more object IDs to process
      if (terminate_ && object_ids_.empty()) {
        return;
      }

      // Create a copy of object IDs to avoid blocking
      object_ids = object_ids_;
      object_ids_.clear();
    }
    tasks_cv_.notify_one();

    ARROW_LOG(DEBUG) << "Dequeued " << object_ids.size() << " requests";

    std::vector<std::string> data, metadata;
    ARROW_CHECK_OK(Get(object_ids, &data, &metadata));

    // Write back to plasma store
    auto client = Client();
    for (size_t i = 0; i < object_ids.size(); ++i) {
      if (data.at(i).empty()) {
        continue;
      }
      std::shared_ptr<Buffer> object_data;
      auto object_metadata = reinterpret_cast<const uint8_t *>(metadata.at(i).empty() ? nullptr : metadata.at(i).data());
      auto data_size = static_cast<int64_t>(data.at(i).size());
      auto metadata_size = static_cast<int64_t>(metadata.at(i).size());
      auto s = client->Create(object_ids.at(i), data_size, object_metadata, metadata_size, &object_data);
      if (s.IsPlasmaObjectExists()) {
        ARROW_LOG(DEBUG) << "Unevicted object " << object_ids.at(i).hex() << " already exists in Plasma store";
        continue;
      }
      ARROW_CHECK_OK(std::move(s));
      if (data_size > OBJECT_SIZE_THRESHOLD) {
        arrow::internal::parallel_memcopy(object_data->mutable_data(),
                                          reinterpret_cast<const uint8_t *>(data.at(i).data()),
                                          data_size,
                                          MEMCPY_BLOCK_SIZE,
                                          MEMCPY_NUM_THREADS);
      } else {
        std::memcpy(object_data->mutable_data(), data.at(i).data(), static_cast<size_t>(data_size));
      }
      ARROW_CHECK_OK(client->Seal(object_ids.at(i)));
      ARROW_CHECK_OK(client->Release(object_ids.at(i)));
    }
  }
}

std::shared_ptr<PlasmaClient> ExternalStoreWorker::Client() {
  if (client_ == nullptr) {
    client_ = std::make_shared<PlasmaClient>();
    ARROW_CHECK_OK(client_->Connect(store_socket_, ""));
  }
  return client_;
}

}