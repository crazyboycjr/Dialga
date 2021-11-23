#ifndef DIALGA_INTERNAL_ALLOC_SYSTEM_H_
#define DIALGA_INTERNAL_ALLOC_SYSTEM_H_
#include <malloc.h>
#include <stdlib.h>

namespace dialga {
namespace alloc {

class System {
 public:
  static void* Alloc(size_t size) { return malloc(size); }
  static void Free(void* addr) { free(addr); }
};

}  // namespace alloc
}  // namespace dialga

#endif  // DIALGA_INTERNAL_ALLOC_SYSTEM_H_
