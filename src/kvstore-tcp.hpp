#ifndef DIALGA_KVSTORE_TCP_HPP_
#define DIALGA_KVSTORE_TCP_HPP_
#include "dialga/kvstore.hpp"
#include "socket/socket.h"

namespace dialga {

class Endpoint {
 public:
 private:
  socket::Socket sock;
};

class KVStoreTcp : public KVStore {
 public:
  virtual ~KVStoreTcp() {}

  int Init() override;

  int Put(const std::vector<Key>& keys,
          const std::vector<Value>& values) override;

  int Get(const std::vector<Key>& keys,
          const std::vector<Value*>& values,
          const Callback& cb = nullptr) override;

  int Delete(const std::vector<Key>& keys,
             const Callback& cb = nullptr) override;

  int Register(const char* buf, size_t size) override;

  void Free(Value* value) override;
};

class KVServerTcp : public KVServer {
 public:
  virtual ~KVServerTcp() {}

  int Init() override;

  int Run() override;
};

}  // namespace dialga

#endif  // DIALGA_KVSTORE_TCP_HPP_
