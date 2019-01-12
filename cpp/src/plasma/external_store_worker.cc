#include <utility>

#include "external_store_worker.h"

namespace plasma {

ExternalStoreWorker::ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                                         const std::string &external_store_endpoint)
    : num_writes_(0),
      num_bytes_written_(0),
      num_reads_not_found_(0),
      num_reads_(0),
      num_bytes_read_(0) {
  if (external_store) {
    valid_ = true;
    for (int i = 0; i < READ_WRITE_PARALLELISM * 2; ++i) { // x2 handles for puts
      external_store_handles_.push_back(external_store->Connect(external_store_endpoint));
    }
  }
}

ExternalStoreWorker::~ExternalStoreWorker() {
  PrintCounters();
}

bool ExternalStoreWorker::IsValid() const {
  return valid_;
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

void ExternalStoreWorker::ParallelGet(const std::vector<ObjectID> &object_ids, std::vector<std::string> &object_data) {
  object_data.resize(object_ids.size());

  int num_objects = static_cast<int>(object_ids.size());
  const ObjectID *ids_ptr = &object_ids[0];
  std::string *data_ptr = &object_data[0];
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

  for (const auto &i : object_data) {
    if (i.empty()) {
      num_reads_not_found_++;
      continue;
    }
    num_reads_++;
    num_bytes_read_ += i.size();
  }
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