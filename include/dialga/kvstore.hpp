#ifndef DIALGA_KVSTORE_HPP_
#define DIALGA_KVSTORE_HPP_
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include "dialga/sarray.hpp"

namespace dialga {

using Key = uint64_t;
// Zero-copy byte string. To enable zero-copy, the data ownership must be
// shared between the user and the network I/O thread. This SArray
// maintains a reference count to allow safely deletion from the user side,
// while still being accessiable by the network I/O thread.
using ZValue = SArray<char>;

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

  /// At least one of these Get or ZGet must be implemented.
  virtual int Get(const std::vector<Key>& keys,
                  std::vector<Value*>& values,
                  const Callback& cb = nullptr) = 0;

  virtual int ZGet(const std::vector<Key>& keys,
                   std::vector<ZValue*>& zvalues,
                   const Callback& cb = nullptr) {
    std::vector<Value*> values;
    values.reserve(zvalues.size());
    for (size_t i = 0; i < values.size(); i++) {
      values.push_back(new Value(0, 0));
    }
    int rc = Get(keys, values, cb);
    if (rc) return rc;
    for (size_t i = 0; i < zvalues.size(); i++) {
      zvalues[i] = new SArray<char>(reinterpret_cast<char*>(values[i]->addr_),
                                    values[i]->size_, false);
    }
    return 0;
  }

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
