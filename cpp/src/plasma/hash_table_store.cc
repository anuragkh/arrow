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

Status HashTableStore::Connect(const std::string &endpoint) {
  return Status::OK();
}

Status HashTableStore::Put(const std::vector<ObjectID> &object_ids,
                           const std::vector<std::string> &object_data,
                           const std::vector<std::string> &object_metadata) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    table_[object_ids.at(i)] = std::make_pair(object_data.at(i), object_metadata.at(i));
  }
  return Status::OK();
}

Status HashTableStore::Get(const std::vector<ObjectID> &object_ids,
                           std::vector<std::string> *object_data,
                           std::vector<std::string> *object_metadata) {
  for (ObjectID object_id : object_ids) {
    auto result = table_.find(object_id);
    if (result != table_.end()) {
      object_data->push_back(result->second.first);
      object_metadata->push_back(result->second.second);
    } else {
      object_data->push_back("");
      object_metadata->push_back("");
    }
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("hashtable", HashTableStore);

}
