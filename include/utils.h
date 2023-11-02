// Contains small utility classes and functions for cpark.

#ifndef CPARK_UTILS_H
#define CPARK_UTILS_H

#include <ostream>

namespace cpark {

namespace utils {

/** An ostream that eats all outputs to it.  */
class NullStream : public std::ostream {
private:
  class NullBuffer : public std::streambuf {
  public:
    int overflow(int c) override { return c; }
  };

  inline static NullBuffer null_buffer_;

public:
  NullStream() : std::ostream(&null_buffer_) {}
};

inline static NullStream g_null_stream;

}  // namespace utils

}  // namespace cpark

#endif  // CPARK_UTILS_H
