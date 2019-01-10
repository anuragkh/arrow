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

#ifndef REDIS_STORE_H
#define REDIS_STORE_H

#include "external_store.h"

#include <cpp_redis/cpp_redis>

namespace plasma {

class RedisStoreHandle : public ExternalStoreHandle {
 public:
  explicit RedisStoreHandle(std::shared_ptr<cpp_redis::client> client);

  Status Put(size_t num_objects, const ObjectID *ids, const std::string *data) override;

  Status Get(size_t num_objects, const ObjectID *ids, std::string *data) override;
 private:
  std::shared_ptr<cpp_redis::client> client_;
};

class RedisStore : public ExternalStore {
 public:
  std::shared_ptr<ExternalStoreHandle> Connect(const std::string &endpoint) override;

 private:
  std::pair<std::string, std::string> ExtractEndpointElements(const std::string &endpoint);
};

}

#endif // REDIS_STORE_H
