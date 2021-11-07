#ifndef DIALGA_KVSTORE_TCP_HPP_
#define DIALGA_KVSTORE_TCP_HPP_
#include "../kvstore.hpp"
#include "socket/socket.h"

namespace dialga {

class Endpoint {
 public:
 private:
  socket::Socket sock;
};

class KVStoreTcp : KVStore {
 public:
  virtual ~KVStoreTcp() {}

  void Init() override;

  void Put(const std::vector<Key>& keys,
           const std::vector<Value>& values,
           const Callback& cb = nullptr) override;

  void Get(const std::vector<Key>& keys,
           const std::vector<Value*>& values,
           const Callback& cb = nullptr) override;

  void Delete(const std::vector<Key>& keys,
              const Callback& cb = nullptr) override;
 private:
};

}  // namespace dialga

#endif  // DIALGA_KVSTORE_TCP_H_
