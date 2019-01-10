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
// The worker maintains a worker thread internally for servicing Get requests.
// All Get requests are enqueued, and periodically serviced by the worker
// thread. All Put requests are serviced by the calling thread directly.
// The worker interface ensures thread-safe access to the external store.

class ExternalStoreWorker {
 public:
  typedef std::vector<ObjectID> object_list;
  typedef object_list::const_iterator object_iterator;

  static const int kDefaultParallelism = 8;
  static const size_t kPerThreadQueueSize = 32;
  static const int kObjectSizeThreshold = 1024 * 1024;
  static const size_t kMemcpyBlockSize = 64;

  ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                      const std::string &external_store_endpoint,
                      const std::string &store_socket,
                      int parallelism = kDefaultParallelism);

  ~ExternalStoreWorker();

  /// Checks if the external store is valid or not.
  ///
  /// \return True if the external store is valid, false otherwise.
  bool IsValid() const;

  /// Get request; once the object has been read from the external
  /// store, it is automatically written back to the Plasma Store.
  ///
  /// \param object_id The object ID corresponding to the Get request.
  /// \return The return status.
  Status GetAndWriteToPlasma(const ObjectID &object_id);

  /// Put an object in the external store.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  /// \param object_metadata The object metadata to put.
  void ParallelPut(const std::vector<ObjectID> &object_ids,
                   const std::vector<std::string> &object_data,
                   const std::vector<std::string> &object_metadata);

  /// Shutdown the external store worker.
  void Shutdown();

  // Reset Counters
  void ResetCounters();

  /// Print statistics
  void PrintCounters();

 private:
  /// Contains the logic for the worker thread.
  void DoWork();

  /// Get objects from external store and writes it back to plasma store.
  ///
  /// \param object_ids The object IDs to get.
  /// \return The return status.
  void ParallelGetAndWriteBack(const std::vector<ObjectID> &object_ids);

  /// Write a chunk of objects to external store. To be used as a task
  /// for a thread pool.
  static Status WriteChunkToExternalStore(std::shared_ptr<ExternalStoreHandle> handle,
                                          size_t num_objects,
                                          const ObjectID *ids,
                                          const std::string *data,
                                          const std::string *metadata);

  /// Read a chunk of objects from external store. To be used as a task
  /// for a thread pool.
  static Status ReadChunkFromExternalStore(std::shared_ptr<ExternalStoreHandle> handle,
                                           size_t num_objects,
                                           const ObjectID *ids,
                                           std::string *data,
                                           std::string *metadata);

  /// Returns a client to the plasma store, creating one if not already initialized.
  ///
  /// @return A client to the plasma store.
  std::shared_ptr<PlasmaClient> Client();

  bool valid_;
  int parallelism_;
  size_t max_enqueue_;
  std::vector<std::shared_ptr<ExternalStoreHandle>> external_store_handles_;

  std::string store_socket_;
  std::shared_ptr<PlasmaClient> plasma_client_;

  std::thread worker_thread_;
  std::vector<ObjectID> object_ids_;

  std::mutex tasks_mutex_;
  std::condition_variable tasks_cv_;
  bool terminate_;
  bool stopped_;

  // Eviction statistics
  size_t num_writes_;
  size_t num_bytes_written_;
  size_t num_reads_not_found_;
  size_t num_reads_;
  size_t num_bytes_read_;
};

}

#endif // EXTERNAL_STORE_WORKER_H
