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

Status HashTableStore::Put(const std::vector<ObjectID> &object_ids, std::vector<ObjectBuffer> &object_buffers) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    table_[object_ids[i]] = std::make_pair(object_buffers[i].data->ToString(), object_buffers[i].metadata->ToString());
  }
  return Status::OK();
}

Status HashTableStore::Get(const std::vector<ObjectID> &object_ids,
                           std::vector<std::string> *object_data,
                           std::vector<std::string> *object_metadata) {
  for (ObjectID object_id : object_ids) {
    auto result = table_[object_id];
    object_data->push_back(result.first);
    object_metadata->push_back(result.second);
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("hashtable", HashTableStore);

}
