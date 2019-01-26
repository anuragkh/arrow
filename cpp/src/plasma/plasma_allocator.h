#ifndef PLASMA_ALLOCATOR_H
#define PLASMA_ALLOCATOR_H

#include <cstddef>
#include <cstdint>

namespace plasma {

class PlasmaAllocator {
 public:
  static void* Memalign(size_t alignment, size_t bytes);
  static void Free(void* mem, size_t bytes);
  static void SetFootprintLimit(size_t bytes);
  static int64_t GetFootprintLimit();
  static int64_t Allocated();
 private:
  static int64_t allocated_;
  static int64_t footprint_limit_;
};

}

#endif //ARROW_PLASMA_ALLOCATOR_H
