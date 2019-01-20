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

#ifndef EXTERNAL_STORE_H
#define EXTERNAL_STORE_H

#include <map>
#include "plasma/client.h"

namespace plasma {

// ==== The external store ====
//
// This file contains declaration for all functions that need to be implemented
// for an external storage service so that objects evicted from Plasma store
// can be written to it.

class ExternalStoreHandle {
 public:
  /// Default constructor.
  ExternalStoreHandle() = default;

  /// Virtual destructor.
  virtual ~ExternalStoreHandle() = default;

  /// This method will be called whenever an object in the Plasma store needs
  /// to be evicted to the external store.
  ///
  /// This API is experimental and might change in the future.
  ///
  /// \param num_objects The number of objects to put.
  /// \param ids The IDs of the objects to put.
  /// \param data The object data to put.
  /// \return The return status.
  virtual Status Put(const std::vector<ObjectID> &ids,
                     const std::vector<std::shared_ptr<Buffer>> &data) = 0;

  /// This method will be called whenever an evicted object in the External
  /// store store needs to be accessed.
  ///
  /// This API is experimental and might change in the future.
  ///
  /// \param num_objects The number of objects to get.
  /// \param ids The IDs of the objects to get.
  /// \param[out] data The object data.
  /// \return The return status.
  virtual Status Get(const std::vector<ObjectID> &ids,
                     std::vector<std::string> &data) = 0;

};

class ExternalStore {
 public:
  /// Default constructor.
  ExternalStore() = default;

  /// Virtual destructor.
  virtual ~ExternalStore() = default;

  /// Connect to the local plasma store. Return the resulting connection.
  ///
  /// \param endpoint The name of the endpoint to connect to the external
  ///        storage service. While the formatting of the endpoint name is
  ///        specific to the implementation of the external store, it always
  ///        starts with {store-name}://, where {store-name} is the name of the
  ///        external store.
  /// \param[out] handle A handle to the external store.
  ///
  /// \return The return status.
  virtual Status Connect(const std::string &endpoint,
                         std::shared_ptr<ExternalStoreHandle> *handle) = 0;
};

class ExternalStores {
 public:
  /// Extracts the external store name from the external store endpoint.
  ///
  /// \param endpoint The endpoint for the external store.
  /// \param[out] store_name The name of the external store.
  /// \return The return status.
  static Status ExtractStoreName(const std::string &endpoint,
                                 std::string &store_name);

  /// Register a new external store.
  ///
  /// \param store_name Name of the new external store.
  /// \param store The new external store object.
  static void RegisterStore(const std::string &store_name,
                            std::shared_ptr<ExternalStore> store);

  /// Register a new external store.
  ///
  /// \param store_name Name of the new external store.
  static void DeregisterStore(const std::string &store_name);

  /// Obtain the external store given its name.
  ///
  /// \param store_name Name of the external store.
  /// \return The external store object.
  static std::shared_ptr<ExternalStore> GetStore(const std::string &store_name);

 private:
  /// Obtain mapping between external store names and store instances.
  ///
  /// \return Mapping between external store names and store instances.
  static std::unordered_map<std::string, std::shared_ptr<ExternalStore>>& Stores();
};

#define REGISTER_EXTERNAL_STORE(name, store)                                  \
  class store##Class {                                                        \
   public:                                                                    \
    store##Class() {                                                          \
      ExternalStores::RegisterStore(name, std::make_shared<store>());         \
    }                                                                         \
    ~store##Class() {                                                         \
      ExternalStores::DeregisterStore(name);                                  \
    }                                                                         \
  };                                                                          \
  store##Class singleton_##store = store##Class()

}

#endif // EXTERNAL_STORE_H
