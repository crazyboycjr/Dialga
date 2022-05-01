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

  /*!
   * \brief Push a key-value list to the server.
   *
   * The callback function is called when the local completion
   * is generated. The caller must not release memory before that.
   * TODO(cjr): Ideally, the Value type should be SArray so that
   * the user need not worry about the lifetime management.
   *
   * The callback function will be invoked when the values are ready.
   * @param list of keys
   * @param list of values
   * @return 0 for success, otherwise it returns an error code.
   */
  virtual int Put(const std::vector<Key>& keys,
                  const std::vector<ZValue>& values,
                  const Callback& cb = nullptr) = 0;

  // TODO(cjr): Extend kvstore APIs, maybe class KVStoreExt: public KVStore;
  // virtual int PutSync(const std::vector<Key>& keys,
  //                     const std::vector<Value>& values) = 0;

  // virtual int PutOne(const Key key,
  //                    const Value& values,
  //                    const Callback& cb = nullptr) = 0;

  /*!
   * \brief Get the values given the list of keys from the server.
   *
   * If the user does not specify the space for values, the user should
   * pass a list of nullptrs. The list of values should have the exact same
   * length as the keys. Alternatively, the user can pre-allocate the space
   * for those values. In this case, the data will be directly put in
   * the addresses specified by the user. In both cases, the user must free
   * the memory of values.
   *
   * The callback function will be invoked when the values are ready.
   *
   * At least one of these Get or ZGet must be implemented.
   * @param list of keys
   * @param list of values
   * @return 0 for success, otherwise it returns an error code.
   */
  virtual int ZGet(const std::vector<Key>& keys,
                   std::vector<ZValue*>* zvalues,
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
