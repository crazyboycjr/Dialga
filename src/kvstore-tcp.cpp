#include "./kvstore-tcp.hpp"

namespace dialga {

int KVStoreTcp::Init() { LOG(FATAL) << "unimplemented"; }

int KVStoreTcp::Put(const std::vector<Key>& keys,
                    const std::vector<Value>& values) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Get(const std::vector<Key>& keys,
                    const std::vector<Value*>& values,
                    const Callback& cb) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Delete(const std::vector<Key>& keys,
                       const Callback& cb) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Register(const char* buf, size_t size) {
  LOG(FATAL) << "unimplemented";
}

void KVStoreTcp::Free(Value* value) {
  LOG(FATAL) << "unimplemented";
}

int KVServerTcp::Init() {
  LOG(FATAL) << "unimplemented";
}

int KVServerTcp::Run() {
  LOG(FATAL) << "unimplemented";
}

}  // namespace dialga
