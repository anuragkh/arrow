// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef EXTERNAL_STORE_WORKER_H
#define EXTERNAL_STORE_WORKER_H

#include <unistd.h>

#include <condition_variable>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "plasma/common.h"
#include "plasma/external_store.h"

namespace plasma {

// ==== The external store worker ====
//
// The worker maintains a thread-pool internally for servicing Get requests.
// All Get requests are enqueued, and periodically serviced by a worker
// thread. All Put requests are serviced using multiple threads.
// The worker interface ensures thread-safe access to the external store.
//
// Note: this implementation uses a custom thread-pool since it is used in ways
// that the Arrow thread-pool doesn't support:
// 1. The implementation maintains a queue of object IDs to fetch, and fetches
//    multiple object IDs per thread to improve performance via batching.
// 2. The implementation uses the thread-pool to detect if too many Get()
//    requests have been enqueued, and sends an error message back to the
//    caller so that the calling thread can try synchronous Get() instead.

class ExternalStoreWorker {
 public:
  static const size_t kMaxEnqueue = 32;
  static const size_t kCopyParallelism = 4;
  static const size_t kObjectSizeThreshold = 1024 * 1024;
  static const size_t kCopyBlockSize = 64;

  /// Constructor.
  ///
  /// @param external_store The external store implementation.
  /// @param external_store_endpoint The external store endpoint to connect to.
  /// @param plasma_store_socket The socket that Plasma clients can connect to.
  /// @param parallelism The number of threads to use for async requests.
  ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                      const std::string& external_store_endpoint,
                      const std::string& plasma_store_socket, size_t parallelism);

  ~ExternalStoreWorker();

  /// Checks if the external store is valid or not.
  ///
  /// \return True if the external store is valid, false otherwise.
  bool IsValid() const;

  /// Get the parallelism for the external store worker.
  ///
  /// \return The degree of parallelism for the external store worker.
  size_t Parallelism();

  /// Put objects in the external store. The function executes synchronously
  /// from the caller thread.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  /// \return The return status.
  Status PutSync(const std::vector<ObjectID>& object_ids,
                 const std::vector<std::shared_ptr<Buffer>>& object_data);

  /// Put objects in the external store. The function immediately returns after
  /// offloading the Put() to a separate thread, but populates a file descriptor
  /// that can be listened on for a notification indicating completion of Put().
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  /// \param[out] fd File descriptor to listen on for completion notification.
  /// \return The return status.
  Status PutAsync(const std::vector<ObjectID>& object_ids,
                  const std::vector<std::shared_ptr<Buffer>>& object_data, int* fd);

  /// Get objects from the external store. Called synchronously from the caller
  /// thread.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param[out] object_data The object data to get.
  /// \return The return status.
  Status Get(const std::vector<ObjectID>& object_ids,
             std::vector<std::string>& object_data);

  /// Copy memory buffer in parallel if data size is large enough.
  ///
  /// \param dst Destination memory buffer.
  /// \param src Source memory buffer.
  /// \param n Number of bytes to copy.
  void CopyBuffer(uint8_t* dst, const uint8_t* src, size_t n);

  /// Enqueue an un-evict request; if the request is successfully enqueued, the
  /// worker thread processes the request, reads the object from external store
  /// and writes it back to plasma.
  ///
  /// \param object_id The object ID corresponding to the un-evict request.
  /// \return The return status.
  Status EnqueueUnevictRequest(const ObjectID& object_id);

  /// Print Counters
  void PrintCounters();

  /// Shutdown the external store worker.
  void Shutdown();

 private:
  /// Get objects from the external store, using the provided handle.
  /// Updates external store read counters.
  ///
  /// @param handle Handle to the external store.
  /// @param object_ids Object IDs to get.
  /// @param object_data Object data to get.
  /// @return The return status.
  Status Get(std::shared_ptr<ExternalStoreHandle> handle,
             const std::vector<ObjectID>& object_ids,
             std::vector<std::string>& object_data);

  /// Contains the logic for a worker thread.
  ///
  /// \param idx The thread ID.
  void DoWork(size_t idx);

  /// Write objects to plasma store.
  ///
  /// \param client The plasma client.
  /// \param object_ids The object IDs to write.
  /// \param data The object data to write.
  /// \return The return status.
  Status WriteToPlasma(std::shared_ptr<PlasmaClient> client,
                       const std::vector<ObjectID>& object_ids,
                       const std::vector<std::string>& data);

  /// Obtain a client to the plasma store for the given thread ID,
  /// creating one if not already initialized.
  ///
  /// \param idx The thread ID.
  /// \param client Client to the plasma store.
  /// \return The return status.
  Status Client(size_t idx, std::shared_ptr<PlasmaClient>* client);

  /// Obtain an async handle to the external store for the given thread ID,
  /// creating one if not already initialized.
  ///
  /// \param idx The thread ID,
  /// \param handle Handle to the external store.
  /// \return The return status.
  Status AsyncHandle(size_t idx, std::shared_ptr<ExternalStoreHandle>* handle);

  /// Obtain a sync handle to the external store, creating it if not initialized.
  ///
  /// \param handle Handle to the external store.
  /// \return The return status.
  Status SyncHandle(std::shared_ptr<ExternalStoreHandle>* handle);

  /// Obtain a write handle to the external store, creating it if not initialized.
  ///
  /// \param handle Handle to the external store.
  /// \return The return status.
  Status WriteHandle(std::shared_ptr<ExternalStoreHandle>* handle);

  // Whether or not plasma is backed by external store
  bool valid_;

  // Plasma store connection
  std::string plasma_store_socket_;
  std::vector<std::shared_ptr<PlasmaClient>> plasma_clients_;

  // External Store handles
  size_t parallelism_;
  std::string external_store_endpoint_;
  std::shared_ptr<ExternalStore> external_store_;
  std::shared_ptr<ExternalStoreHandle> sync_handle_;
  std::shared_ptr<ExternalStoreHandle> write_handle_;
  std::vector<std::shared_ptr<ExternalStoreHandle>> async_handles_;

  // Worker thread
  std::vector<std::thread> thread_pool_;
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

}  // namespace plasma

#endif  // EXTERNAL_STORE_WORKER_H