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
    // kv = new RdmaKVStore();
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

// TODO(cjr): move the following to C_API
// int KVStore::Get(const std::vector<Key>& keys,
//                  std::vector<Value*>& values,
//                  const Callback& cb) {
//   std::vector<ZValue*> zvalues;
//   zvalues.reserve(values.size());
//   for (auto v : values) {
//     if (v) {
//       // user provided buffers
//       CHECK(v->addr_ && v->size_ > 0)
//           << "User should not provide an invalid buffer";
//       zvalues.push_back(
//           new SArray<char>(reinterpret_cast<char*>(v->addr_), v->size_, false));
//     } else {
//       // network library provided zero copy buffers
//       zvalues.push_back(nullptr);
//     }
//   }
//   int rc = ZGet(keys, zvalues, [&]() {
//     for (size_t i = 0; i < zvalues.size(); i++) {
//       if (!values[i]) {
//         // Copy the data only when use does not provide buffers
//         auto v = static_cast<char*>(malloc(zvalues[i]->bytes()));
//         memcpy(v, zvalues[i]->data(), zvalues[i]->bytes());
//       }
//       delete zvalues[i];
//     }
// 
//     cb();
//   });
//   return rc;
// }

}  // namespace dialga
