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

#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <random>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/test-util.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/external_store.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test-util.h"

namespace plasma {

std::string external_test_executable;  // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer& object_buffer,
                             const std::string& metadata,
                             const std::string& data) {
  arrow::AssertBufferEqual(*object_buffer.metadata, metadata);
  arrow::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStoreWithExternal : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.
  void SetUp() {
    uint64_t seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 rng(static_cast<uint32_t>(seed));
    std::string store_index = std::to_string(rng());
    store_socket_name_ = "/tmp/store" + store_index;

    std::string plasma_directory =
        external_test_executable.substr(0, external_test_executable.find_last_of("/"));
    std::string plasma_command = plasma_directory +
                                 "/plasma_store_server -m 104857600 -e hashtable://test -s " +
                                 store_socket_name_ + " 1> /tmp/log.stdout 2> /tmp/log.stderr &";
    system(plasma_command.c_str());
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
  }
  virtual void TearDown() {
    ARROW_CHECK_OK(client_.Disconnect());
    // Kill all plasma_store processes
    // TODO should only kill the processes we launched
#ifdef COVERAGE_BUILD
    // Ask plasma_store to exit gracefully and give it time to write out
    // coverage files
    system("killall -TERM plasma_store_server");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    system("killall -KILL plasma_store_server");
  }

 protected:
  PlasmaClient client_;
  std::string store_socket_name_;
};

TEST_F(TestPlasmaStoreWithExternal, EvictionTest) {
  // Create many objects on the first client. Seal one third, abort one third,
  // and leave the last third unsealed.
  std::vector<ObjectID> object_ids;
  std::string data(10 * 1024 * 1024, 'x');
  std::string metadata(1, char(5));
  for (int i = 0; i < 11; i++) {
    ObjectID object_id = random_object_id();
    object_ids.push_back(object_id);

    // Test for object non-existence.
    bool has_object;
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_FALSE(has_object);

    // Test for the object being in local Plasma store.
    // Create and seal the object.
    ARROW_CHECK_OK(client_.CreateAndSeal(object_id, data, metadata));
    // Test that the client can get the object.
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_TRUE(has_object);
  }

  // Check to make sure that the first two objects have been evicted
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_ids.at(0), &has_object));
  ASSERT_FALSE(has_object);

  ARROW_CHECK_OK(client_.Contains(object_ids.at(1), &has_object));
  ASSERT_FALSE(has_object);

  for (int i = 0; i < 11; i++) {
    // Since we are accessing objects sequentially, every object we
    // access would be a cache "miss" owing to LRU eviction.

    // Try and access object from plasma store, without trying external store
    // This should fail to fetch the object.
    std::vector<ObjectBuffer> object_buffers;
    ARROW_CHECK_OK(client_.Get({object_ids[i]}, 0, &object_buffers));
    ASSERT_EQ(object_buffers.size(), 1);
    ASSERT_EQ(object_buffers[0].device_num, 0);
    ASSERT_FALSE(object_buffers[0].data);

    // Try and access the object from the plasma store first, and then try
    // external store on failure. This should succeed to fetch the object.
    // However, it may evict the next few objects.
    ARROW_CHECK_OK(client_.GetTryExternal({object_ids[i]}, -1, &object_buffers));
    ASSERT_EQ(object_buffers.size(), 1);
    ASSERT_EQ(object_buffers[0].device_num, 0);
    ASSERT_TRUE(object_buffers[0].data);
    AssertObjectBufferEqual(object_buffers[0], metadata, data);
  }

  // Make sure we still cannot fetch objects that do not exist
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client_.GetTryExternal({random_object_id()}, 100, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 0);
  ASSERT_EQ(object_buffers[0].data, nullptr);
  ASSERT_EQ(object_buffers[0].metadata, nullptr);

  // Check (again) to make sure that the first two objects have been evicted
  ARROW_CHECK_OK(client_.Contains(object_ids.at(0), &has_object));
  ASSERT_FALSE(has_object);

  ARROW_CHECK_OK(client_.Contains(object_ids.at(1), &has_object));
  ASSERT_FALSE(has_object);

  // Try to manually unevict objects
  client_.TryUnevictObjects({object_ids.at(0), object_ids.at(1)});

  for (int i = 0; i < 2; i++) {
    // Try and access object from plasma store, without trying external store
    // This should succeed to fetch the object.
    std::vector<ObjectBuffer> object_buffers;
    ARROW_CHECK_OK(client_.Get({object_ids[i]}, -1, &object_buffers));
    ASSERT_EQ(object_buffers.size(), 1);
    ASSERT_EQ(object_buffers[0].device_num, 0);
    ASSERT_TRUE(object_buffers[0].data);
    AssertObjectBufferEqual(object_buffers[0], metadata, data);
  }
}

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::external_test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
