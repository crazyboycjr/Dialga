#ifndef DIALGA_KVSTORE_HPP_
#define DIALGA_KVSTORE_HPP_
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dialga {

using Key = uint64_t;
// using Value = std::string;
//  TODO: Value is described as the address + size; Since it is pre-registered,
//  the corresponding lkey and rkey can be easily read.
class Value {
 public:
  uint64_t addr_;
  size_t size_;
  Value(uint64_t addr, size_t size) : addr_(addr), size_(size) {}
};

/* \brief: Callback to allow pipeline. */
using Callback = std::function<void()>;

class KVStore {
 public:
  virtual ~KVStore() {}

  static KVStore* Create(const char* type = "local");

  virtual int Init() = 0;

  virtual int Put(const std::vector<Key>& keys,
                  const std::vector<Value>& values) = 0;

  virtual int Get(const std::vector<Key>& keys,
                  const std::vector<Value*>& values,
                  const Callback& cb = nullptr) = 0;

  virtual int Delete(const std::vector<Key>& keys,
                     const Callback& cb = nullptr) = 0;

  virtual int Register(const char* buf, size_t size) = 0;

  virtual void Free(Value* value) = 0;
};

class KVServer {
 public:
  virtual ~KVServer() {}

  static KVServer* Create(const char* type = "local");

  virtual int Init() = 0;

  virtual int Run() = 0;
};

}  // namespace dialga

#endif  // DIALGA_KVSTORE_HPP_
