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

#include "redis_store.h"

namespace plasma {

Status RedisStore::Connect(const std::string &endpoint) {
  auto ep = ExtractEndpointElements(endpoint);
  ARROW_LOG(INFO) << "Connecting to Redis host=" << ep.first << ", port=" << ep.second;
  client_ = std::make_shared<cpp_redis::client>();
  client_->connect(ep.first, std::stoull(ep.second),
                   [](const std::string &host, std::size_t port, cpp_redis::client::connect_state status) {
                     if (status == cpp_redis::client::connect_state::dropped
                         || status == cpp_redis::client::connect_state::failed
                         || status == cpp_redis::client::connect_state::lookup_failed) {
                       ARROW_LOG(ERROR) << "Redis client disconnected from " << host << ":" << port;
                     }
                   });
  return Status::OK();
}

Status RedisStore::Put(const std::vector<ObjectID> &object_ids,
                       const std::vector<std::string> &object_data,
                       const std::vector<std::string> &object_metadata) {
  std::vector<std::future<cpp_redis::reply>> futures;
  for (size_t i = 0; i < object_ids.size(); ++i) {
    futures.push_back(client_->set(object_ids[i].binary(), SerializeValue(object_data[i], object_metadata[i])));
  }

  client_->commit();

  bool err = false;
  std::string err_msg;
  for (auto &fut: futures) {
    auto r = fut.get();
    if (r.is_error()) {
      err = true;
      err_msg += r.error() + "\n";
      ARROW_LOG(DEBUG) << "Redis Put Error: " << r.error();
    }
  }
  return err ? Status::IOError(err_msg) : Status::OK();
}

Status RedisStore::Get(const std::vector<ObjectID> &object_ids,
                       std::vector<std::string> *object_data,
                       std::vector<std::string> *object_metadata) {
  std::vector<std::future<cpp_redis::reply>> futures;
  futures.reserve(object_ids.size());
  for (const ObjectID &object_id: object_ids) {
    futures.push_back(client_->get(object_id.binary()));
  }

  client_->commit();

  for (auto &fut: futures) {
    auto r = fut.get();
    if (r.is_error() || r.is_null()) {
      object_data->push_back("");
      object_metadata->push_back("");
      if (r.is_error()) {
        ARROW_LOG(DEBUG) << "Redis Get Error: " << r.error();
      } else {
        ARROW_LOG(DEBUG) << "Redis Get NotFound";
      }
    } else {
      auto value = DeserializeValue(r.as_string());
      object_data->push_back(value.first);
      object_metadata->push_back(value.second);
    }
  }

  return Status::OK();
}

std::pair<std::string, std::string> RedisStore::ExtractEndpointElements(const std::string &endpoint) {
  std::string separator = "://";
  std::size_t pos = endpoint.find(separator);
  if (pos == std::string::npos) {
    throw std::invalid_argument("Malformed endpoint " + endpoint);
  }
  // Decompose endpoint into URI (redis) and path elements
  std::string uri = endpoint.substr(0, pos);
  std::size_t conn_ep_pos = pos + separator.length();
  std::size_t conn_ep_len = endpoint.length() - separator.length() - uri.length();
  std::string conn_ep = endpoint.substr(conn_ep_pos, conn_ep_len);

  auto host_end = std::find(conn_ep.begin(), conn_ep.end(), ':');
  return std::make_pair(std::string(conn_ep.begin(), host_end),
                        (host_end == conn_ep.end()) ? "6379" : std::string(host_end + 1, conn_ep.end()));
}

REGISTER_EXTERNAL_STORE("redis", RedisStore);

}