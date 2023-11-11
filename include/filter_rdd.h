#ifndef CPARK_FILTER_RDD_H
#define CPARK_FILTER_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
 * An Rdd holding the data filtered from an old rdd by some function.
 * @tparam R Type of the old Rdd.
 * @tparam Func Type of the transformation function.
 * The type of the old Rdd `R`'s elements should be able to invoke function `Func`,
 * and the type shouldn't change after filter.
 */
template <concepts::Rdd R, typename Func>
requires std::invocable<Func, utils::RddElementType<R>>&& std::is_convertible_v<
    std::invoke_result_t<Func, utils::RddElementType<R>>, bool> class FilterRdd
    : public BaseRdd<FilterRdd<R, Func>> {
public:
  using Base = BaseRdd<FilterRdd<R, Func>>;
  friend Base;

public:
  /**
   * A lazily evaluated iterator that can filter values from some original iterator.
   * Because std::ranges::filter_view does not provide `begin() const` and `end() const`,
   * we can not safely rely on it to implement FilterRdd without breaking const-correctness.
   * So we define our own filter iterator here.
   */
  class Iterator : std::random_access_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = utils::RddElementType<R>;

    using OriginalIterator = std::ranges::iterator_t<std::ranges::range_value_t<R>>;
    using OriginalSentinel = std::ranges::sentinel_t<std::ranges::range_value_t<R>>;

    Iterator() = default;

    /** Creates the iterator with function `func` and starting argument value `i`. */
    Iterator(OriginalIterator iterator, OriginalSentinel sentinel, Func* func)
        : iterator_{std::move(iterator)}, sentinel_{std::move(sentinel)}, func_{func} {
      // Find next element that satisfy the predication by `func_`.
      iterator_ = std::ranges::find_if(iterator_, sentinel_, *func_);
    }

    /** Computes the current value. */
    // Not sure if returning value_type would satisfy input_range requirements.
    // Works for now.
    value_type operator*() const { return *iterator_; }

    /** Increments the iterator to the next element satisfying `func_`. */
    Iterator& operator++() {
      ++iterator_;
      // Find next element that satisfy the predication by `func_`.
      iterator_ = std::ranges::find_if(iterator_, sentinel_, *func_);
      return *this;
    }

    /** Increments the iterator to the next element. */
    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    /** Compound addition operator. */
    Iterator& operator+=(difference_type n) {
      if (n >= 0)
        while (n > 0) {
          ++(*this);
          --n;
        }
      else
        while (n < 0) {
          --(*this);
          ++n;
        }
      return *this;
    }

    /** Compound subtraction operator. */
    Iterator& operator-=(difference_type n) {
      return *this += -n;
    }

    /** Addition operator. */
    Iterator operator+(difference_type n) const {
      Iterator temp = *this;
      temp += n;
      return temp;
    }

    /** Subtraction operator. */
    Iterator operator-(difference_type n) const {
      Iterator temp = *this;
      temp -= n;
      return temp;
    }

    /** Subscript operator. */
    value_type& operator[](difference_type n) const {
      return *(*this + n);
    }

    /** Equality operator. */
    bool operator==(const Iterator& other) const { return iterator_ == other.iterator_; }

    /** Inequality operator. */
    bool operator!=(const Iterator& other) const { return !(*this == other); }

    /** Less than operator. */
    bool operator<(const Iterator& other) const {
      return iterator_ < other.iterator_;
    }

    /** Greater than operator. */
    bool operator>(const Iterator& other) const {
      return iterator_ > other.iterator_;
    }

    /** Less than or equal to operator. */
    bool operator<=(const Iterator& other) const {
      return iterator_ <= other.iterator_;
    }

    /** Greater than or equal to operator. */
    bool operator>=(const Iterator& other) const {
      return iterator_ >= other.iterator_;
    }

    /** Difference operator. */
    difference_type operator-(const Iterator& other) const {
      return iterator_ - other.iterator_;
    }

  private:
    OriginalIterator iterator_;
    OriginalSentinel sentinel_;
    Func* func_;
  };

  constexpr FilterRdd(const R& prev, Func func) : Base{prev, false}, func_{std::move(func)} {
    static_assert(concepts::Rdd<FilterRdd<R, Func>>,
                  "Instance of FilterRdd does not satisfy Rdd concept.");
    // Create the filtered splits.
    for (const concepts::Split auto& prev_split : prev) {
      splits_.emplace_back(
          std::ranges::subrange{
              Iterator{std::ranges::begin(prev_split), std::ranges::end(prev_split), &func_},
              Iterator{std::ranges::end(prev_split), std::ranges::end(prev_split), &func_}},
          prev_split);
      splits_.back().addDependency(prev_split);
    }
  }

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr FilterRdd(const FilterRdd&) = default;
  FilterRdd& operator=(const FilterRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  std::vector<ViewSplit<std::ranges::subrange<Iterator>>> splits_{};
  Func func_;
};

/**
 * Helper class to create Transformed Rdd with pipeline operator `|`.
 */
template <typename Func>
class Filter {
public:
  explicit Filter(Func func) : func_{std::move(func)} {}

  template <concepts::Rdd R, typename T = utils::RddElementType<R>>
  requires std::invocable<Func, T>&& std::is_same_v<std::invoke_result_t<Func, T>, bool> auto
  operator()(const R& r) const {
    return FilterRdd(r, func_);
  }

private:
  Func func_;
};

/**
 * Helper function to create Filter Rdd with pipeline operator `|`.
 */
template <typename Func, concepts::Rdd R>
auto operator|(const R& r, const Filter<Func>& filter) {
  return filter(r);
}

}  // namespace cpark

#endif  //CPARK_FILTER_RDD_H
