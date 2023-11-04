// Contains small utility classes and functions for cpark.

#ifndef CPARK_UTILS_H
#define CPARK_UTILS_H

#include <ostream>

namespace cpark {

namespace utils {

/** An ostream that eats all outputs.  */
class NullOStream : public std::ostream {
private:
  class NullBuffer : public std::streambuf {
  public:
    int overflow(int c) override { return c; }
  };

  inline static NullBuffer g_null_buffer_;

public:
  NullOStream() : std::ostream(&g_null_buffer_) {}
};

inline static NullOStream g_null_ostream;

template <typename R>
using RddElementType = std::ranges::range_value_t<std::ranges::range_value_t<R>>;

}  // namespace utils

}  // namespace cpark

#endif  // CPARK_UTILS_H
