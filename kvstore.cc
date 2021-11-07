#include "kvstore.hpp"
#include "tcp/kvstore-tcp.hpp"

#include <iostream>

namespace dialga {

KVStore* KVStore::Create(const char* type) {
  KVStore* kv = nullptr;
  auto type_str = std::string(type);
  if (type_str == "tcp") {
    kv = new KVStoreTcp();
  } else {
    // TODO(cjr): change to GLOG
    std::cerr << "unknown store type: " << type_str << std::endl;
  }
  return kv;
}

}  // namespace dialga
