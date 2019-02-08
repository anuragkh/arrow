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

#ifndef JIFFY_STORE_H
#define JIFFY_STORE_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <jiffy/client/jiffy_client.h>

#include "plasma/external_store.h"

namespace plasma {

class JiffyStore : public ExternalStore {
 public:
  JiffyStore() = default;

  Status Connect(const std::string& endpoint) override;

  Status Get(const std::vector<ObjectID>& ids,
             std::vector<std::shared_ptr<Buffer>> buffers) override;

  Status Put(const std::vector<ObjectID>& ids,
             const std::vector<std::shared_ptr<Buffer>>& data) override;

 private:
  std::tuple<std::string, int, int> ExtractEndpointElements(const std::string &endpoint);

  std::shared_ptr<jiffy::client::jiffy_client> client_;
  std::shared_ptr<jiffy::storage::hash_table_client> hash_table_client_;
};

}  // namespace plasma

#endif  // JIFFY_STORE_H
