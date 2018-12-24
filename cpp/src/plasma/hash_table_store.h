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

#ifndef HASH_TABLE_STORE_H
#define HASH_TABLE_STORE_H

#include "external_store.h"

namespace plasma {

class HashTableStore : public ExternalStore {
 public:
  HashTableStore() noexcept = default;

  Status Connect(const std::string &endpoint) override;

  Status Put(const std::vector<ObjectID> &object_ids, std::vector<ObjectBuffer> &object_buffers) override;

  Status Get(const std::vector<ObjectID> &object_ids,
             std::vector<std::string> *object_data,
             std::vector<std::string> *object_metadata) override;

 private:
  std::unordered_map<ObjectID, std::pair<std::string, std::string>> table_;
};

}

#endif // HASH_TABLE_STORE_H
