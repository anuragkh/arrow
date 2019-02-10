#include <plasma/thirdparty/libjiffy/src/jiffy/utils/string_utils.h>
#include "plasma/jiffy_store.h"
#include "arrow/util/logging.h"
#include "jiffy_store.h"

namespace plasma {

Status plasma::JiffyStore::Connect(const std::string &endpoint) {
  try {
    auto ep = ExtractEndpointElements(endpoint);
    ARROW_LOG(INFO) << "Connecting to Jiffy @ " << std::get<0>(ep) << ":" << std::get<1>(ep) << ":" << std::get<2>(ep);
    client_ = std::make_shared<jiffy::client::jiffy_client>(std::get<0>(ep), std::get<1>(ep), std::get<2>(ep));
    hash_table_client_ = client_->open_or_create_hash_table("/tmp", "local://tmp", 4);
  } catch (std::exception &e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

Status plasma::JiffyStore::Get(const std::vector<plasma::ObjectID> &ids, std::vector<std::shared_ptr<Buffer>> buffers) {
  // Convert ObjectIds to strings
  std::vector<std::string> keys(ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    keys[i] = ids[i].binary();
  }
  std::vector<std::string> values;
  try {
    values = hash_table_client_->get(keys);
  } catch (std::exception &e) {
    return Status::IOError(e.what());
  }
  ARROW_CHECK(values.size() == ids.size());
  buffers.resize(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    ARROW_CHECK(buffers[i]->size() == static_cast<int64_t>(values[i].size()));
    std::memcpy(buffers[i]->mutable_data(), values[i].data(), values[i].size());
  }

  return Status::OK();
}

Status plasma::JiffyStore::Put(const std::vector<plasma::ObjectID> &ids,
                               const std::vector<std::shared_ptr<Buffer>> &data) {
  // Convert ObjectIds, buffers to strings
  std::vector<std::string> kvs(ids.size() * 2);
  for (size_t i = 0; i < ids.size(); ++i) {
    kvs[2 * i] = ids[i].binary();
    kvs[2 * i + 1] = data[i]->ToString();
  }

  try {
    hash_table_client_->put(kvs);
  } catch (std::exception &e) {
    return Status::IOError(e.what());
  }

  return Status::OK();
}

std::tuple<std::string, int, int> plasma::JiffyStore::ExtractEndpointElements(const std::string &endpoint) {
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

  auto parts = jiffy::utils::string_utils::split(conn_ep, ':');
  return std::make_tuple(parts.at(0), std::stoi(parts.at(1)), std::stoi(parts.at(2)));
}

Status JiffyStore::Disconnect() {
  try {
    client_->remove("/tmp");
  } catch (std::exception &e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("jiffy", JiffyStore);

}
