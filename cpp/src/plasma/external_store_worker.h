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
  explicit ExternalStoreWorker(std::shared_ptr<ExternalStore> external_store,
                               const std::string &store_socket);

  ~ExternalStoreWorker();

  /// Checks if the external store is valid or not.
  ///
  /// \return True if the external store is valid, false otherwise.
  bool IsValid() const;

  /// Enqueues a Get request; once the object has been read from the remote
  /// store, it is automatically written back to the Plasma Store.
  ///
  /// \param object_id The object ID corresponding to the Get request.
  void GetAndWriteToPlasma(const ObjectID &object_id);

  /// Put an object in the external store.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  /// \param object_metadata The object metadata to put.
  /// \return The return status.
  void Put(const std::vector<ObjectID> &object_ids,
           const std::vector<std::string> &object_data,
           const std::vector<std::string> &object_metadata);

  /// Shutdown the external store worker.
  void Shutdown();

 private:
  /// Contains the logic for the worker thread.
  void DoWork();

  /// Returns a client to the plasma store, creating one if not already initialized.
  ///
  /// @return A client to the plasma store.
  PlasmaClient *Client();

  std::shared_ptr<ExternalStore> external_store_;
  PlasmaClient *client_;
  std::string store_socket_;

  std::thread worker_thread_;
  std::vector<ObjectID> object_ids_;
  std::vector<std::string> data_;
  std::vector<std::string> metadata_;

  std::mutex tasks_mutex_;
  std::mutex store_mutex_;
  std::condition_variable condition_;
  bool terminate_;
  bool stopped_;
};

}

#endif // EXTERNAL_STORE_WORKER_H
