#ifndef DIALGA_WIRE_HPP_
#define DIALGA_WIRE_HPP_
#include "dialga/kvstore.hpp"
#include "dialga/sarray.hpp"
#include <cstdint>

namespace dialga {

enum Operation : uint32_t {
  PUT = 1,
  GET = 2,
  DELETE = 3,
};

// On wire data.
// meta | key 0 | key 1 | ... | len 0 | len 1 | ... | value 0 | value 1 | ...
struct KVMeta {
  /*! \brief operation */
  Operation op;
  /*! \brief number of keys */
  uint32_t num_keys;
  /*! \brief an identifier to differentiate requests with same set of keys,
   * and must be passed back by the response */
  uint64_t timestamp;
};

static_assert(sizeof(KVMeta) == 16, "sizeof(KVMeta) != 16");

struct KVPairs {
  SArray<Key> keys;
  SArray<uint32_t> lens;
  std::vector<SArray<char>> values;

  explicit KVPairs() {}

  explicit KVPairs(const KVMeta& meta) {
    // TODO(cjr): These are just for server side. for client side in the
    // future, distinguish the PUT_REQ, PUT_RES, GET_REQ, GET_RES
    if (meta.op == Operation::PUT) {
      // allocate the space for PUT operation
      keys = SArray<Key>(meta.num_keys);
      lens = SArray<uint32_t>(meta.num_keys);
      values.clear();
      values.reserve(meta.num_keys);
      for (uint32_t i = 0; i < meta.num_keys; i++) {
        values[i] = SArray<char>(lens[i] / sizeof(char));
      }
    } else if (meta.op == Operation::GET || meta.op == Operation::DELETE) {
      // for GET and DELETE, only `keys` is non-empty.
      keys = SArray<Key>(meta.num_keys);
      lens.clear();
      values.clear();
    } else {
      CHECK(0) << "unexpected op: " << static_cast<uint32_t>(meta.op);
    }
  }
};



}  // namespace dialga

#endif  // DIALGA_WIRE_HPP_
