#include <iostream>
#include <glog/logging.h>

#include "dialga/kvstore.hpp"
#include "./kvstore-rdma.hpp"
#include "./kvstore-tcp.hpp"
#include "./kvserver-tcp.hpp"

namespace dialga {

KVStore* KVStore::Create(const char* type) {
  KVStore* kv = nullptr;
  auto type_str = std::string(type);
  if (type_str == "rdma") {
    kv = new RdmaKVStore();
  } else if (type_str == "tcp") {
    kv = new KVStoreTcp();
  } else {
    LOG(FATAL) << "unknown store type: " << type;
  }
  return kv;
}

KVServer* KVServer::Create(const char* type) {
  KVServer* kv = nullptr;
  auto type_str = std::string(type);
  if (type_str == "rdma") {
    kv = new RdmaKVServer();
  } else if (type_str == "tcp") {
    kv = new KVServerTcp();
  } else {
    LOG(FATAL) << "unknown store type: " << type;
  }
  return kv;
}

}  // namespace dialga
