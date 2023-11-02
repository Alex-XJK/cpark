#ifndef CPARK_GENERATOR_RDD_H
#define CPARK_GENERATOR_RDD_H

#include "rdd.h"

namespace cpark {

/**
 * An Rdd that generates a sequence of data from a number range.
 * @tparam Num The type of the number composing the range. It must be arithmetic
 * types.
 * @tparam Func The function that maps the number into some data. Must be
 * invokable from Num.
 * @tparam T The data type of the Rdd. Must be convertible from the result of
 * Func.
 */
template <typename Num, typename Func, typename T = std::invoke_result_t<Func, Num>>
requires std::is_arithmetic_v<Num>&& std::invocable<Func, Num>&&
    std::convertible_to<std::invoke_result_t<Func, Num>, T> class GeneratorRdd
    : public BaseRdd<GeneratorRdd<Num, Func, T>> {
public:
  friend BaseRdd<GeneratorRdd<Num, Func, T>>;

  constexpr GeneratorRdd(Num begin, Num end, Func func)
      : func_{std::move(func)}, begin_{begin}, end_{end} {
    static_assert(Rdd<GeneratorRdd<Num, Func, T>>,
                  "Instance of GeneratorRdd does not satisfy Rdd concept.");
  }

  class iterator : std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = T;

    iterator() = default;

    iterator(const Func* func, Num i) : func_{func}, i_{i} {}

    // Not sure if returning value_type would satisfy input_range requirements.
    // Works for now.
    value_type operator*() const { return (*func_)(i_); }

    iterator& operator++() {
      ++i_;
      return *this;
    }

    iterator operator++(int) {
      auto old = *this;
      ++i_;
      return old;
    }

    bool operator==(const iterator& other) const { return func_ == other.func_ && i_ == other.i_; }

    bool operator!=(const iterator& other) const { return !(*this == other); }

  private:
    const Func* func_;
    Num i_{};
  };

private:
  constexpr auto get_split_impl(size_t i) const {
    size_t size = (end_ - begin_);
    size_t split_size = size / splits_;
    auto b = begin(), e = begin();
    std::ranges::advance(b, i * split_size);
    std::ranges::advance(e, std::min(size, (i + 1) * split_size));
    return std::ranges::subrange(b, e);
  }

  constexpr size_t splits_num_impl() const { return splits_; }

public:
  constexpr iterator begin() const { return {&func_, begin_}; }

  constexpr iterator end() const { return {&func_, end_}; }

private:
  Func func_;
  Num begin_, end_;
  size_t splits_{8};
};

}  // namespace cpark

#endif //CPARK_GENERATOR_RDD_H
