#include <utility>

#include "external_store_worker.h"
#include "arrow/util/memory.h"

namespace plasma {

ExternalStoreWorker::ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                                         const std::string &external_store_endpoint,
                                         const std::string &store_socket)
    : store_socket_(store_socket),
      plasma_client_(nullptr),
      terminate_(false),
      stopped_(false) {
  num_writes_ = 0;
  num_bytes_written_ = 0;
  num_reads_not_found_ = 0;
  num_reads_ = 0;
  num_bytes_read_ = 0;
  if (external_store) {
    valid_ = true;
    for (int i = 0; i < READ_WRITE_PARALLELISM * 2; ++i) { // x2 handles for puts
      external_store_handles_.push_back(external_store->Connect(external_store_endpoint));
    }
    worker_thread_ = std::thread(&ExternalStoreWorker::DoWork, this);
  }
}

ExternalStoreWorker::~ExternalStoreWorker() {
  PrintCounters();
  if (!stopped_) {
    Shutdown();
  }
}

bool ExternalStoreWorker::IsValid() const {
  return valid_;
}

Status ExternalStoreWorker::GetAndWriteToPlasma(const ObjectID &object_id) {
  size_t n_enqueued = 0;
  {
    std::unique_lock<std::mutex> lock(tasks_mutex_);
    if (object_ids_.size() >= (READ_WRITE_PARALLELISM * PER_THREAD_QUEUE_SIZE)) {
      return Status::CapacityError("Too many un-evict requests");
    }
    object_ids_.push_back(object_id);
    n_enqueued = object_ids_.size();
  }
  tasks_cv_.notify_one();
  ARROW_LOG(DEBUG) << "Enqueued " << n_enqueued << " requests";
  return Status::OK();
}

void ExternalStoreWorker::ParallelPut(const std::vector<ObjectID> &object_ids,
                                      const std::vector<std::string> &object_data) {
  int num_objects = static_cast<int>(object_ids.size());
  const ObjectID *ids_ptr = &object_ids[0];
  const std::string *data_ptr = &object_data[0];

#if READ_WRITE_PARALLELISM > 1
  int num_chunks = std::min(READ_WRITE_PARALLELISM, num_objects);
  int chunk_size = num_objects / num_chunks;
  int last_chunk_size = num_objects - (chunk_size * (num_chunks - 1));

  std::vector<std::future<Status>> futures;
  for (int i = 0; i < num_chunks; ++i) {
    auto chunk_size_i = i == (num_chunks - 1) ? last_chunk_size : chunk_size;
    futures.push_back(std::async(&ExternalStoreWorker::WriteChunkToExternalStore,
                                 external_store_handles_[READ_WRITE_PARALLELISM + i],
                                 chunk_size_i,
                                 ids_ptr + i * chunk_size,
                                 data_ptr + i * chunk_size));
  }

  for (auto &fut: futures) {
    ARROW_CHECK_OK(fut.get());
  }
#else
  ARROW_CHECK_OK(ExternalStoreWorker::WriteChunkToExternalStore(external_store_handles_.back(),
                                                                num_objects,
                                                                ids_ptr,
                                                                data_ptr));
#endif

  num_writes_ += num_objects;
  for (const auto &i : object_data) {
    num_bytes_written_ += i.size();
  }
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

void ExternalStoreWorker::ResetCounters() {
  num_writes_ = 0;
  num_bytes_written_ = 0;
  num_reads_ = 0;
  num_bytes_read_ = 0;
  num_reads_not_found_ = 0;
}

void ExternalStoreWorker::PrintCounters() {
  // Print statistics
  ARROW_LOG(INFO) << "External Store Counters: ";
  ARROW_LOG(INFO) << "Number of objects written: " << num_writes_;
  ARROW_LOG(INFO) << "Number of bytes written: " << num_bytes_written_;
  ARROW_LOG(INFO) << "Number of objects read: " << num_reads_;
  ARROW_LOG(INFO) << "Number of bytes read: " << num_bytes_read_;
  ARROW_LOG(INFO) << "Number of objects attempted to read, but not found: " << num_reads_not_found_;
}

void ExternalStoreWorker::ParallelGetAndWriteBack(const std::vector<ObjectID> &object_ids) {
  std::vector<std::string> data;
  data.resize(object_ids.size());

  int num_objects = static_cast<int>(object_ids.size());
  const ObjectID *ids_ptr = &object_ids[0];
  std::string *data_ptr = &data[0];
#if READ_WRITE_PARALLELISM > 1
  int num_chunks = std::min(READ_WRITE_PARALLELISM, num_objects);
  int chunk_size = num_objects / num_chunks;
  int last_chunk_size = num_objects - (chunk_size * (num_chunks - 1));



  std::vector<std::future<Status>> futures;
  for (int i = 0; i < num_chunks; ++i) {
    auto chunk_size_i = i == (num_chunks - 1) ? last_chunk_size : chunk_size;
    futures.push_back(std::async(&ExternalStoreWorker::ReadChunkFromExternalStore,
                                 external_store_handles_[i],
                                 chunk_size_i,
                                 ids_ptr + i * chunk_size,
                                 data_ptr + i * chunk_size));
  }

  for (auto &fut: futures) {
    ARROW_CHECK_OK(fut.get());
  }
#else
  ARROW_CHECK_OK(ExternalStoreWorker::ReadChunkFromExternalStore(external_store_handles_.front(),
                                                                 num_objects,
                                                                 ids_ptr,
                                                                 data_ptr));
#endif
  // Write back to plasma store
  auto client = Client();
  for (size_t i = 0; i < object_ids.size(); ++i) {
    if (data.at(i).empty()) {
      num_reads_not_found_++;
      continue;
    }
    std::shared_ptr<Buffer> object_data;
    auto data_size = static_cast<int64_t>(data.at(i).size());
    auto s = client->Create(object_ids.at(i), data_size, nullptr, 0, &object_data);
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
                                        MEMCPY_PARALLELISM);
    } else {
      std::memcpy(object_data->mutable_data(), data.at(i).data(), static_cast<size_t>(data_size));
    }
    ARROW_CHECK_OK(client->Seal(object_ids.at(i)));
    ARROW_CHECK_OK(client->Release(object_ids.at(i)));
    num_reads_++;
    num_bytes_read_ += data_size;
  }
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
    ParallelGetAndWriteBack(object_ids);
  }
}

std::shared_ptr<PlasmaClient> ExternalStoreWorker::Client() {
  if (plasma_client_ == nullptr) {
    plasma_client_ = std::make_shared<PlasmaClient>();
    ARROW_CHECK_OK(plasma_client_->Connect(store_socket_, ""));
  }
  return plasma_client_;
}

Status ExternalStoreWorker::WriteChunkToExternalStore(std::shared_ptr<ExternalStoreHandle> handle,
                                                      size_t num_objects,
                                                      const ObjectID *ids,
                                                      const std::string *data) {
  return handle->Put(num_objects, ids, data);
}

Status ExternalStoreWorker::ReadChunkFromExternalStore(std::shared_ptr<ExternalStoreHandle> handle,
                                                       size_t num_objects,
                                                       const ObjectID *ids,
                                                       std::string *data) {
  return handle->Get(num_objects, ids, data);
}

}