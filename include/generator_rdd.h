#ifndef CPARK_GENERATOR_RDD_H
#define CPARK_GENERATOR_RDD_H

#include "base_rdd.h"

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
  using Base = BaseRdd<GeneratorRdd<Num, Func, T>>;
  friend Base;

  /**
   * An iterator that can be used to compute the value of a function `Func` invoked by a `Num`.
   */
  class Iterator : std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = T;

    Iterator() = default;

    /** Creates the iterator with function `func` and starting argument value `i`. */
    Iterator(const Func* func, Num i) : func_{func}, i_{i} {}

    /** Computes the current value `func_(i_)`. */
    // Not sure if returning value_type would satisfy input_range requirements.
    // Works for now.
    value_type operator*() const { return (*func_)(i_); }

    /** Increments the function argument. */
    Iterator& operator++() {
      ++i_;
      return *this;
    }

    /** Increments the function argument. */
    Iterator operator++(int) {
      auto old = *this;
      ++i_;
      return old;
    }

    /** Two iterators equal each other if and only if they have the same function and argument. */
    bool operator==(const Iterator& other) const { return func_ == other.func_ && i_ == other.i_; }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    const Func* func_;
    Num i_{};
  };

public:
  constexpr GeneratorRdd(Num begin, Num end, Func func, ExecutionContext* context)
      : Base{context}, func_{std::move(func)}, begin_{begin}, end_{end} {
    static_assert(concepts::Rdd<GeneratorRdd<Num, Func, T>>,
                  "Instance of GeneratorRdd does not satisfy Rdd concept.");

    // Creates the splits for this Rdd. Each split contains two iterators that can be used to
    // compute the corresponding value.
    for (size_t i : std::views::iota(size_t{0}, Base::splits_num_)) {
      size_t total_size = static_cast<size_t>(end_ - begin_);
      size_t split_size = (total_size + Base::splits_num_ - 1) / Base::splits_num_;
      auto b = Iterator{&func, begin_ + static_cast<Num>(std::min(total_size, i * split_size))};
      auto e =
          Iterator{&func, begin_ + static_cast<Num>(std::min(total_size, (i + 1) * split_size))};
      splits_.emplace_back(std::ranges::subrange{b, e}, Base::context_);
    }
  }

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr GeneratorRdd(const GeneratorRdd&) = default;
  GeneratorRdd& operator=(const GeneratorRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  Func func_;
  Num begin_, end_;
  std::vector<ViewSplit<std::ranges::subrange<Iterator>>> splits_{};
};

}  // namespace cpark

#endif  //CPARK_GENERATOR_RDD_H
