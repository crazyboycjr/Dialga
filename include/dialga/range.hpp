/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef DIALGA_RANGE_HPP_
#define DIALGA_RANGE_HPP_
#include <cstdint>
namespace dialga {

/**
 * \brief a range [begin, end)
 */
class Range {
 public:
  Range() : Range(0, 0) {}
  Range(uint64_t begin, uint64_t end) : begin_(begin), end_(end) { }

  uint64_t begin() const { return begin_; }
  uint64_t end() const { return end_; }
  uint64_t size() const { return end_ - begin_; }
 private:
  uint64_t begin_;
  uint64_t end_;
};

}  // namespace dialga
#endif  // DIALGA_RANGE_HPP_
