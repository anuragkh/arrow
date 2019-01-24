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

#ifndef S3_STORE_H
#define S3_STORE_H

#include "external_store.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

namespace plasma {

class S3StoreHandle : public ExternalStoreHandle {
 public:
  S3StoreHandle(const Aws::String &bucket,
                const Aws::String &key_prefix,
                const Aws::Client::ClientConfiguration &config);

  Status Put(const std::vector<ObjectID> &ids, const std::vector<std::shared_ptr<Buffer>> &data) override;
  Status Get(const std::vector<ObjectID> &ids, std::vector<std::string> &data) override;

 private:
  Aws::String bucket_name_;
  Aws::String key_prefix_;
  Aws::S3::S3Client client_;
};

class S3Store : public ExternalStore {
 public:
  S3Store();
  ~S3Store() override;
  Status Connect(const std::string &endpoint, std::shared_ptr<ExternalStoreHandle> *handle) override;

 private:
  std::pair<Aws::String, Aws::String> ExtractEndpointElements(const std::string &s3_endpoint);

  Aws::SDKOptions options_;
  Aws::Client::ClientConfiguration config_;
};

}

#endif // PLASMA_S3_STORE_H
