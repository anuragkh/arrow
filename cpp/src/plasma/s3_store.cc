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

#include "s3_store.h"

#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/utils/stream/SimpleStreamBuf.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/core/config/AWSProfileConfigLoader.h>

#include "arrow/util/logging.h"

namespace plasma {

using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

static Aws::String GetRegion() {
  auto region = std::getenv("AWS_REGION");
  if (region == nullptr) {
    return Aws::String("us-east-1"); // Default region
  }
  return Aws::String(region);
}

S3StoreHandle::S3StoreHandle(const Aws::String &bucket,
                             const Aws::String &key_prefix,
                             const ClientConfiguration &config)
    : bucket_name_(bucket), key_prefix_(key_prefix), client_(config) {
}

Status S3StoreHandle::Put(const std::vector<ObjectID> &ids,
                          const std::vector<std::shared_ptr<Buffer>> &data) {
  std::string err_msg;
  for (size_t i = 0; i < ids.size(); ++i) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucket_name_).WithKey(key_prefix_ + ids[i].hex().data());
    auto objectStream = Aws::MakeShared<Aws::StringStream>("DataStream");
    *objectStream << data[i]->ToString();
    objectStream->flush();
    request.SetBody(objectStream);

    auto outcome = client_.PutObject(request);
    if (!outcome.IsSuccess())
      err_msg += std::string(outcome.GetError().GetMessage().data()) + "\n";
  }
  return err_msg.empty() ? Status::OK() : Status::IOError(err_msg);
}

Status S3StoreHandle::Get(const std::vector<ObjectID> &ids, std::vector<std::string> &data) {
  data.resize(ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name_).WithKey(key_prefix_ + ids[i].hex().data());
    auto outcome = client_.GetObject(request);
    if (!outcome.IsSuccess())
      throw std::runtime_error(outcome.GetError().GetMessage().c_str());
    auto in = std::make_shared<Aws::IOStream>(outcome.GetResult().GetBody().rdbuf());
    data[i].assign(std::istreambuf_iterator<char>(*in), std::istreambuf_iterator<char>());
  }
  return Status::OK();
}

std::pair<Aws::String, Aws::String> S3Store::ExtractEndpointElements(const std::string &s3_endpoint) {
  std::string separator = ":/";
  std::size_t pos = s3_endpoint.find(separator);
  if (pos == std::string::npos) {
    throw std::invalid_argument("Malformed endpoint " + s3_endpoint);
  }
  // Decompose endpoint into URI (s3) and path elements
  std::string uri = s3_endpoint.substr(0, pos);
  std::size_t s3_path_pos = pos + separator.length();
  std::size_t s3_path_len = s3_endpoint.length() - separator.length() - uri.length();
  std::string s3_path = s3_endpoint.substr(s3_path_pos, s3_path_len);

  // Decompose path into bucket and key-prefix elements
  auto s3_bucket_end = std::find(s3_path.begin() + 1, s3_path.end(), '/');
  Aws::String s3_bucket = Aws::String(s3_path.begin() + 1, s3_bucket_end);
  Aws::String s3_key_prefix = (s3_bucket_end == s3_path.end()) ? "" : Aws::String(s3_bucket_end + 1, s3_path.end());

  // Return bucket and key-prefix
  return std::make_pair(s3_bucket, s3_key_prefix);
}

S3Store::S3Store() {
  Aws::InitAPI(options_);
  config_.region = GetRegion();
}

S3Store::~S3Store() {
  Aws::ShutdownAPI(options_);
}

Status S3Store::Connect(const std::string &endpoint, std::shared_ptr<ExternalStoreHandle> *handle) {
  try {
    auto path_elements = ExtractEndpointElements(endpoint);
    ARROW_LOG(INFO) << "Connecting to s3 bucket \"" << path_elements.first
                    << "\" with key-prefix \"" << path_elements.second << "\"";
    *handle = std::make_shared<S3StoreHandle>(path_elements.first, path_elements.second, config_);
  } catch (std::exception &e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("s3", S3Store);

}

