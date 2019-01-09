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

#include "hash_table_store.h"

namespace plasma {

std::shared_ptr<ExternalStoreHandle> HashTableStore::Connect(const std::string &endpoint) {
  return std::make_shared<HashTableStoreHandle>(table_, mtx_);
}

HashTableStoreHandle::HashTableStoreHandle(hash_table_t &table, std::mutex &mtx)
    : table_(table), mtx_(mtx) {
}

Status HashTableStoreHandle::Put(const std::vector<ObjectID> &object_ids,
                                 const std::vector<std::string> &object_data,
                                 const std::vector<std::string> &object_metadata) {
  return Put(object_ids.size(), &object_ids[0], &object_data[0], &object_metadata[0]);
}


Status HashTableStoreHandle::Put(size_t num_objects,
                                 const ObjectID *object_ids,
                                 const std::string *object_data,
                                 const std::string *object_metadata) {
  for (size_t i = 0; i < num_objects; ++i) {
    std::lock_guard<std::mutex> lock(mtx_);
    table_[object_ids[i]] = std::make_pair(object_data[i], object_metadata[i]);
  }
  return Status::OK();
}

Status HashTableStoreHandle::Get(const std::vector<ObjectID> &object_ids,
                                 std::vector<std::string> *object_data,
                                 std::vector<std::string> *object_metadata) {
  object_data->resize(object_ids.size());
  object_metadata->resize(object_ids.size());
  return Get(object_ids.size(), &object_ids[0], &(*object_data)[0], &(*object_metadata)[0]);
}

Status HashTableStoreHandle::Get(size_t num_objects,
                                 const ObjectID *object_ids,
                                 std::string *object_data,
                                 std::string *object_metadata) {
  for (size_t i = 0; i < num_objects; ++i) {
    bool valid;
    hash_table_t::iterator result;
    {
      std::lock_guard<std::mutex> lock(mtx_);
      result = table_.find(object_ids[i]);
      valid = result != table_.end();
    }
    if (valid) {
      object_data[i] = result->second.first;
      object_metadata[i] = result->second.second;
    } else {
      object_data[i].clear();
      object_metadata[i].clear();
    }
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("hashtable", HashTableStore);

}
