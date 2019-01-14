#ifndef EXTERNAL_STORE_WORKER_H
#define EXTERNAL_STORE_WORKER_H

#include <unistd.h>

#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>

#include "common.h"
#include "external_store.h"

#define MEMCPY_PARALLELISM      4
#define PER_THREAD_QUEUE_SIZE   32
#define OBJECT_SIZE_THRESHOLD   (1024 * 1024)
#define MEMCPY_BLOCK_SIZE       64

namespace plasma {

// ==== The external store worker ====
//
/// The worker maintains a worker thread internally for servicing Get requests.
// All Get requests are enqueued, and periodically serviced by the worker
// thread. All Put requests are serviced by the calling thread directly.
// The worker interface ensures thread-safe access to the external store.

class ExternalStoreWorker {
 public:
  ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                      const std::string &external_store_endpoint,
                      const std::string &plasma_store_socket,
                      size_t parallelism);

  ~ExternalStoreWorker();

  /// Checks if the external store is valid or not.
  ///
  /// \return True if the external store is valid, false otherwise.
  bool IsValid() const;

  /// Put objects in the external store; uses multiple threads.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  void ParallelPut(const std::vector<ObjectID> &object_ids,
                   const std::vector<std::string> &object_data);

  /// Get objects from the external store; uses multiple threads.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param object_data[out] The object data to get.
  void ParallelGet(const std::vector<ObjectID> &object_ids,
                   std::vector<std::string> &object_data);

  /// Get objects from the external store.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param object_data[out] The object data to get.
  void SequentialGet(const std::vector<ObjectID> &object_ids,
                     std::vector<std::string> &object_data);

  /// Compy memory in parallel if data size is large enough
  ///
  /// @param dst Destination memory buffer
  /// @param src Source memory buffer
  /// @param n Number of bytes to copy
  void ParallelMemcpy(uint8_t *dst, const uint8_t *src, size_t n);

  /// Enqueue an un-evict request; if the request is successfully enqueued, the
  /// worker thread processes the request, reads the object from external store
  /// and writes it back to plasma.
  ///
  /// \param object_id The object ID corresponding to the un-evict request.
  /// \return True if the request is enqueued successfully, false if there are
  ///         too many requests enqueued already.
  bool EnqueueUnevictRequest(const ObjectID &object_id);

  /// Reset Counters
  void ResetCounters();

  /// Print Counters
  void PrintCounters();

  /// Shutdown the external store worker.
  void Shutdown();

 private:
  /// Write a chunk of objects to external store. To be used as a task
  /// for a thread pool.
  static Status PutChunk(std::shared_ptr<ExternalStoreHandle> handle,
                         size_t num_objects,
                         const ObjectID *ids,
                         const std::string *data);

  /// Read a chunk of objects from external store. To be used as a task
  /// for a thread pool.
  static Status GetChunk(std::shared_ptr<ExternalStoreHandle> handle,
                         size_t num_objects,
                         const ObjectID *ids,
                         std::string *data);

  /// Contains the logic for the worker thread.
  void DoWork();

  /// Get objects from external store and writes it back to plasma store.
  ///
  /// \param object_ids The object IDs to get.
  /// \return The return status.
  void ParallelWriteToPlasma(const std::vector<ObjectID> &object_ids, const std::vector<std::string> &data);

  /// Returns a client to the plasma store, creating one if not already initialized.
  ///
  /// @return A client to the plasma store.
  std::shared_ptr<PlasmaClient> Client();

  // Whether or not plasma is backed by external store
  bool valid_;

  // Plasma store connection
  std::string plasma_store_socket_;
  std::shared_ptr<PlasmaClient> plasma_client_;

  // External Store handles
  size_t parallelism_;
  std::shared_ptr<ExternalStoreHandle> sync_handle_;
  std::vector<std::shared_ptr<ExternalStoreHandle>> read_handles_;
  std::vector<std::shared_ptr<ExternalStoreHandle>> write_handles_;

  // Worker thread
  std::thread worker_thread_;
  std::mutex tasks_mutex_;
  std::condition_variable tasks_cv_;
  bool terminate_;
  bool stopped_;

  // Enqueued object IDs
  std::vector<ObjectID> object_ids_;

  // External store read/write statistics
  std::atomic_size_t num_writes_;
  std::atomic_size_t num_bytes_written_;
  std::atomic_size_t num_reads_not_found_;
  std::atomic_size_t num_reads_;
  std::atomic_size_t num_bytes_read_;
};

}

#endif // EXTERNAL_STORE_WORKER_H
