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

/** Get the type of the element inside an Rdd. */
template <typename R>
using RddElementType = std::ranges::range_value_t<std::ranges::range_value_t<R>>;

template <typename R>
using RddKeyType = decltype(std::declval<RddElementType<R>>().first);

template <typename R>
using RddValueType = decltype(std::declval<RddElementType<R>>().second);

namespace {

template <typename Iterator>
struct GetIteratorTagImpl {};

template <typename Iterator>
requires std::forward_iterator<Iterator> struct GetIteratorTagImpl<Iterator> {
  using type = std::forward_iterator_tag;
};

template <typename Iterator>
requires std::bidirectional_iterator<Iterator> struct GetIteratorTagImpl<Iterator> {
  using type = std::bidirectional_iterator_tag;
};

template <typename Iterator>
requires std::random_access_iterator<Iterator> struct GetIteratorTagImpl<Iterator> {
  using type = std::random_access_iterator_tag;
};

} // namespace

/** Get the tag for the iterator type of the `Iterator`. */
template <typename Iterator>
using GetIteratorTagType = typename GetIteratorTagImpl<Iterator>::type;

}  // namespace utils

}  // namespace cpark

#endif  // CPARK_UTILS_H
