#ifndef KVSTORE_KVSTORE_HPP_
#define KVSTORE_KVSTORE_HPP_
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdmatools.hpp"

namespace kvstore {

using Key = uint64_t;
// using Value = std::string;
//  TODO: Value is described as the address + size; Since it is pre-registered,
//  the corresponding lkey and rkey can be easily read.
class Value {
 public:
  uint64_t addr_;
  size_t size_;
};

/* \brief: Callback to allow pipeline. */
using Callback = std::function<void()>;

class IndexEntry {
 public:
  IndexEntry(int qp_index, uint64_t addr, size_t size)
      : qp_index_(qp_index), addr_(addr), size_(size) {}
  // which host(qp) owns the content
  int qp_index_;
  // the virtual addr of the content
  uint64_t addr_;
  uint32_t rkey_;
  // the size of the content
  size_t size_;
};

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
  virtual int Register(char* buf, size_t size) = 0;

 protected:
  std::unordered_map<Key, IndexEntry> indexs_;
};

}  // namespace kvstore

#endif  // KVSTORE_KVSTORE_H_
