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
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/utils/stream/SimpleStreamBuf.h>

namespace plasma {

class S3Store : public ExternalStore {
 public:
  S3Store();
  ~S3Store() override;
  Status Connect(const std::string &endpoint) override;
  Status Put(const std::vector<ObjectID> &object_ids,
             const std::vector<std::string> &object_data,
             const std::vector<std::string> &object_metadata) override;
  Status Get(const std::vector<ObjectID> &object_ids,
             std::vector<std::string> *object_data,
             std::vector<std::string> *object_metadata) override;

 private:
  Aws::S3::Model::PutObjectRequest MakePutRequest(const ObjectID &key, const std::string &object_data,
      const std::string &object_metadata) const;
  Aws::S3::Model::GetObjectRequest MakeGetRequest(const ObjectID &key) const;
  std::pair<std::string, std::string> ParseGetResponse(Aws::S3::Model::GetObjectOutcome &outcome) const;
  void ParsePutResponse(Aws::S3::Model::PutObjectOutcome &outcome) const;

  std::pair<Aws::String, Aws::String> ExtractEndpointElements(const std::string &s3_endpoint);

  Aws::String bucket_name_;
  Aws::String key_prefix_;
  std::shared_ptr<Aws::S3::S3Client> client_;
  Aws::SDKOptions options_;
};

}

#endif // PLASMA_S3_STORE_H
