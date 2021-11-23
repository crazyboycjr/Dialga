#ifndef DIALGA_INTERNAL_ALLOC_ALLOCATOR_HPP_
#define DIALGA_INTERNAL_ALLOC_ALLOCATOR_HPP_

#include "./system.hpp"

namespace dialga {
namespace alloc {

template <typename T, typename MM>
class TypedAllocator {
 public:
  static T* Alloc(size_t num) {
    return static_cast<T*>(MM::Alloc(num * sizeof(T)));
  }
  static void Free(T* addr) { MM::Free(addr); }
};

template <typename T>
using DefaultAllocator = TypedAllocator<T, System>;

}  // namespace alloc
}  // namespace dialga

#endif  // DIALGA_INTERNAL_ALLOC_ALLOCATOR_HPP_
