#ifndef KVSTORE_KVSTORE_H_
#define KVSTORE_KVSTORE_H_
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace kvstore {

using Key = uint64_t;
using Value = std::string;

/* \brief: Callback to allow pipeline. */
using Callback = std::function<void()>;

class KVStore {
 public:
  virtual ~KVStore() {}

  static KVStore* Create(const char* type = "local");

  virtual void Init() = 0;

  virtual void Put(const std::vector<Key>& keys,
                   const std::vector<Value>& values,
                   const Callback& cb = nullptr) = 0;

  virtual void Get(const std::vector<Key>& keys,
                   const std::vector<Value*>& values,
                   const Callback& cb = nullptr) = 0;

  virtual void Delete(const std::vector<Key>& keys,
                      const Callback& cb = nullptr) = 0;
};

}  // namespace kvstore

#endif  // KVSTORE_KVSTORE_H_
