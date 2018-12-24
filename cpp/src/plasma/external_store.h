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
#include "client.h"

namespace plasma {

// ==== The external store ====
//
// This file contains declaration for all functions that need to be implemented
// for an external storage service so that objects evicted from Plasma store
// can be written to it.

class ExternalStore {
 public:
  /// Virtual destructor
  virtual ~ExternalStore() noexcept = default;

  /// Connect to the local plasma store. Return the resulting connection.
  ///
  /// \param endpoint The name of the endpoint to connect to the external
  ///        storage service. While the formatting of the endpoint name is
  ///        specific to the implementation of the external store, it always
  ///        starts with {store-name}://, where {store-name} is the name of the
  ///        external store.
  ///
  /// \return The return status.
  virtual Status Connect(const std::string &endpoint) = 0;

  /// This method will be called whenever an object in the Plasma store needs
  /// to be evicted to the external store.
  ///
  /// \param object_ids The IDs of the objects to put.
  /// \param object_data The object data to put.
  /// \param object_metadata The object metadata to put.
  /// \return The return status.
  virtual Status Put(const std::vector<ObjectID> &object_ids,
                     const std::vector<std::string> &object_data,
                     const std::vector<std::string> &object_metadata) = 0;

  /// This method will be called whenever an evicted object in the External
  /// store store needs to be accessed.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param[out] object_data The object data.
  /// \param[out] object_metadata The object metadata.
  /// \return The return status.
  virtual Status Get(const std::vector<ObjectID> &object_ids,
                     std::vector<std::string> *object_data,
                     std::vector<std::string> *object_metadata) = 0;

 protected:
  /// Serializes the ObjectBuffer to a single binary string
  ///
  /// \param buffer The ObjectBuffer to serialize.
  /// \return The serialized value as a binary string.
  std::string SerializeValue(const std::string &object_data, const std::string &object_metadata) const;

  /// Deserializes a binary string into the object data and metadata.
  ///
  /// \param binary The binary string to deserialize.
  /// \return The deserialized string pair containing the object data and metadata.
  std::pair<std::string, std::string> DeserializeValue(const std::string &binary) const;
};

class ExternalStores {
 public:
  /// Extracts the external store name from the external store endpoint
  ///
  /// @param endpoint The endpoint for the external store
  /// @return The external store name
  static std::string ExtractStoreName(const std::string &endpoint);

  /// Register a new external store.
  ///
  /// @param store_name Name of the new external store.
  /// @param store The new external store object.
  static void RegisterStore(const std::string &store_name,
                            ExternalStore *store);

  /// Register a new external store.
  ///
  /// @param store_name Name of the new external store.
  static ExternalStore *DeregisterStore(const std::string &store_name);

  /// Obtain the external store given its name.
  ///
  /// @param store_name Name of the external store.
  /// @return The external store object.
  static ExternalStore *GetStore(const std::string &store_name);

 private:
  /// Obtain the mapping between external store names and external store instances
  /// @return The mapping between external store names and external store instances
  static std::shared_ptr<std::map<std::string, ExternalStore *>> Stores();

  /// Mapping between external store names and external store instances
  static std::shared_ptr<std::map<std::string, ExternalStore *>> external_stores_;
};

#define REGISTER_EXTERNAL_STORE(name, store)                                  \
  class store##Class {                                                        \
   public:                                                                    \
    store##Class() {                                                          \
      ExternalStores::RegisterStore(name, new store());                       \
    }                                                                         \
    ~store##Class() {                                                         \
      ExternalStore* s = ExternalStores::DeregisterStore(name);               \
      delete s;                                                               \
    }                                                                         \
  };                                                                          \
  store##Class singleton_##store = store##Class()

}

#endif // EXTERNAL_STORE_H
