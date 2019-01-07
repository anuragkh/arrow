#include "external_store_worker.h"

#define MAX_ENQUEUE 32

namespace plasma {

ExternalStoreWorker::ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                                         const std::string &store_socket)
    : external_store_(external_store),
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

Status ExternalStoreWorker::GetAndWriteToPlasma(const ObjectID &object_id) {
  std::string data, metadata;
  auto s = Get(object_id, data, metadata);

  if (!s.IsPlasmaObjectNonexistent()) {
    size_t n_enqueued = 0;
    {
      std::unique_lock<std::mutex> lock(tasks_mutex_);
      if (object_ids_.size() > MAX_ENQUEUE) {
        return Status::CapacityError("Cannot enqueue any more un-evict requests");
      }
      object_ids_.push_back(object_id);
      data_.push_back(data);
      metadata_.push_back(metadata);
      n_enqueued = object_ids_.size();
    }
    tasks_cv_.notify_one();
    ARROW_LOG(DEBUG) << "Enqueued " << n_enqueued << " requests";
  }
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
    std::vector<std::string> data, metadata;
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

      // Create a copy of data to avoid blocking
      data = data_;
      data_.clear();

      // Create a copy of metadata to avoid blocking
      metadata = metadata_;
      metadata_.clear();
    }
    tasks_cv_.notify_one();

    ARROW_LOG(DEBUG) << "Dequeued " << object_ids.size() << " requests";

    // Write back to plasma store
    for (size_t i = 0; i < object_ids.size(); ++i) {
      auto s = Client()->CreateAndSeal(object_ids.at(i), data.at(i), metadata.at(i));
      if (s.IsPlasmaObjectExists()) {
        ARROW_LOG(DEBUG) << "Unevicted object " << object_ids.at(i).hex() << " already exists in Plasma store";
      }
    }
  }
}

PlasmaClient *ExternalStoreWorker::Client() {
  if (client_ == nullptr) {
    client_ = new PlasmaClient();
    ARROW_CHECK_OK(client_->Connect(store_socket_, ""));
  }
  return client_;
}

}