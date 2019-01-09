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

#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>

namespace plasma {

using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

S3Store::S3Store() {
  Aws::InitAPI(options_);
}

S3Store::~S3Store() {
  Aws::ShutdownAPI(options_);
}

std::shared_ptr<ExternalStoreHandle> S3Store::Connect(const std::string &endpoint) {
  ARROW_LOG(INFO) << "Connecting to s3 bucket \"" << bucket_name_ << "\" with key-prefix \"" << key_prefix_ << "\"";

  ClientConfiguration config;
  auto path_elements = ExtractEndpointElements(endpoint);
  return std::make_shared<S3StoreHandle>(path_elements.first,
                                         path_elements.second,
                                         Aws::MakeShared<S3Client>("S3Store", config));
}

S3StoreHandle::S3StoreHandle(const Aws::String &bucket,
                             const Aws::String &key_prefix,
                             std::shared_ptr<Aws::S3::S3Client> client)
    : bucket_name_(bucket), key_prefix_(key_prefix), client_(std::move(client)) {}

Status S3StoreHandle::Put(const std::vector<ObjectID> &object_ids,
                          const std::vector<std::string> &object_data,
                          const std::vector<std::string> &object_metadata) {
  return Put(object_ids.size(), &object_ids[0], &object_data[0], &object_metadata[0]);
}

Status S3StoreHandle::Get(const std::vector<ObjectID> &object_ids,
                          std::vector<std::string> *object_data,
                          std::vector<std::string> *object_metadata) {
  object_data->resize(object_ids.size());
  object_metadata->resize(object_ids.size());
  return Get(object_ids.size(), &object_ids[0], &(*object_data)[0], &(*object_metadata)[0]);
}

Status S3StoreHandle::Put(size_t num_objects,
                          const ObjectID *object_ids,
                          const std::string *object_data,
                          const std::string *object_metadata) {
  for (size_t i = 0; i < num_objects; ++i) {
    auto outcome = client_->PutObject(MakePutRequest(object_ids[i], object_data[i], object_metadata[i]));
    ParsePutResponse(outcome);
  }
  return Status::OK();
}

Status S3StoreHandle::Get(size_t num_objects,
                          const ObjectID *object_ids,
                          std::string *object_data,
                          std::string *object_metadata) {
  for (size_t i = 0; i < num_objects; ++i) {
    auto outcome = client_->GetObject(MakeGetRequest(object_ids[i]));
    auto response = ParseGetResponse(outcome);
    object_data[i] = response.first;
    object_metadata[i] = response.second;
  }
  return Status::OK();
}

Aws::S3::Model::PutObjectRequest S3StoreHandle::MakePutRequest(const ObjectID &key,
                                                               const std::string &object_data,
                                                               const std::string &object_metadata) const {
  Aws::S3::Model::PutObjectRequest request;
  request.WithBucket(bucket_name_).WithKey(key_prefix_ + key.binary().data());

  auto objectStream = Aws::MakeShared<Aws::StringStream>("DataStream");
  *objectStream << SerializeValue(object_data, object_metadata);
  objectStream->flush();
  request.SetBody(objectStream);
  return request;
}

Aws::S3::Model::GetObjectRequest S3StoreHandle::MakeGetRequest(const ObjectID &key) const {
  Aws::S3::Model::GetObjectRequest request;
  request.WithBucket(bucket_name_).WithKey(key_prefix_ + key.binary().data());
  return request;
}

std::pair<std::string, std::string> S3StoreHandle::ParseGetResponse(Aws::S3::Model::GetObjectOutcome &outcome) const {
  if (!outcome.IsSuccess())
    throw std::runtime_error(outcome.GetError().GetMessage().c_str());
  auto in = std::make_shared<Aws::IOStream>(outcome.GetResult().GetBody().rdbuf());
  std::string binary = std::string(std::istreambuf_iterator<char>(*in), std::istreambuf_iterator<char>());
  return DeserializeValue(binary);
}

void S3StoreHandle::ParsePutResponse(Aws::S3::Model::PutObjectOutcome &outcome) const {
  if (!outcome.IsSuccess())
    throw std::runtime_error(outcome.GetError().GetMessage().c_str());
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

REGISTER_EXTERNAL_STORE("s3", S3Store);

}

