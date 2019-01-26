#include <arrow/util/logging.h>

#include "plasma/plasma_allocator.h"
#include "plasma/malloc.h"
#include "plasma_allocator.h"

namespace plasma {

extern "C" {
void* dlmemalign(size_t alignment, size_t bytes);
void dlfree(void* mem);
size_t dlmalloc_set_footprint_limit(size_t bytes);
}

int64_t PlasmaAllocator::footprint_limit_ = 0;
int64_t PlasmaAllocator::allocated_ = 0;

void *PlasmaAllocator::Memalign(size_t alignment, size_t bytes) {
  if (allocated_ + bytes > footprint_limit_) {
    return nullptr;
  }
  void* mem = dlmemalign(alignment, bytes);
  ARROW_CHECK(mem);
  allocated_ += bytes;
  return mem;
}

void PlasmaAllocator::Free(void *mem, size_t bytes) {
  dlfree(mem);
  allocated_ -= bytes;
}

void PlasmaAllocator::SetFootprintLimit(size_t bytes) {
  footprint_limit_ = static_cast<int64_t>(bytes);
}

int64_t PlasmaAllocator::GetFootprintLimit() {
  return footprint_limit_;
}

int64_t PlasmaAllocator::Allocated() {
  return allocated_;
}

}