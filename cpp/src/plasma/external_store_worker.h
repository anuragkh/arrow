#ifndef EXTERNAL_STORE_WORKER_H
#define EXTERNAL_STORE_WORKER_H

#include <unistd.h>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>

#include "common.h"
#include "external_store.h"

namespace plasma {

// ==== The external store worker ====
//
// The worker services external store Get and Put requests using multiple
// threads. The worker interface ensures thread-safe access to the external store.

#define READ_WRITE_PARALLELISM  1

class ExternalStoreWorker {
 public:
  ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                      const std::string &external_store_endpoint);

  ~ExternalStoreWorker();

  /// Checks if the external store is valid or not.
  ///
  /// \return True if the external store is valid, false otherwise.
  bool IsValid() const;

  /// Put an object in the external store.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  /// \param object_metadata The object metadata to put.
  void ParallelPut(const std::vector<ObjectID> &object_ids,
                   const std::vector<std::string> &object_data);

  void ParallelGet(const std::vector<ObjectID> &object_ids,
                   std::vector<std::string> &object_data);

  // Reset Counters
  void ResetCounters();

  /// Print statistics
  void PrintCounters();

 private:
  /// Write a chunk of objects to external store. To be used as a task
  /// for a thread pool.
  static Status WriteChunkToExternalStore(std::shared_ptr<ExternalStoreHandle> handle,
                                          size_t num_objects,
                                          const ObjectID *ids,
                                          const std::string *data);

  /// Read a chunk of objects from external store. To be used as a task
  /// for a thread pool.
  static Status ReadChunkFromExternalStore(std::shared_ptr<ExternalStoreHandle> handle,
                                           size_t num_objects,
                                           const ObjectID *ids,
                                           std::string *data);

  bool valid_;
  std::vector<std::shared_ptr<ExternalStoreHandle>> external_store_handles_;

  // Eviction statistics
  size_t num_writes_;
  size_t num_bytes_written_;
  size_t num_reads_not_found_;
  size_t num_reads_;
  size_t num_bytes_read_;
};

}

#endif // EXTERNAL_STORE_WORKER_H
