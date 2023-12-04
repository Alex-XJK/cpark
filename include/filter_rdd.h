#ifndef CPARK_FILTER_RDD_H
#define CPARK_FILTER_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/** @ingroup g_trans
 *  @defgroup t_Filter Filter
 *  This forms the Filter Transformation of our cpark library
 *  @image html filter.drawio.png "FilterRdd diagram" width=50%
 *  @see FilterRdd
 *  @see Filter
 *  @{
 */

/**
 * A new view created from an original view (V) filtered by some predication function (Func).
 * It has almost the same usage as std::ranges::filter_view, except that it provides
 * `begin() const` and `end() const`.
 */
template <std::ranges::view V, typename Func>
requires std::invocable<Func, std::ranges::range_value_t<V>>&& std::is_convertible_v<
    std::invoke_result_t<Func, std::ranges::range_value_t<V>>, bool> class FilterView
    : public std::ranges::view_interface<FilterView<V, Func>> {
public:
  using OriginalIterator = std::ranges::iterator_t<V>;
  using OriginalSentinel = std::ranges::sentinel_t<V>;

  /**
   * A lazily evaluated iterator that can filter values from some original iterator.
   * Because std::ranges::filter_view does not provide `begin() const` and `end() const`,
   * we can not safely rely on it to implement FilterRdd without breaking const-correctness.
   * So we define our own filter iterator here.
   */
  class Iterator : std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = std::ranges::range_value_t<V>;

    using OriginalIterator = std::ranges::iterator_t<V>;
    using OriginalSentinel = std::ranges::sentinel_t<V>;

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

    /** Equality operator. */
    bool operator==(const Iterator& other) const { return iterator_ == other.iterator_; }

    /** Inequality operator. */
    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    OriginalIterator iterator_;
    OriginalSentinel sentinel_;
    Func* func_;
  };

public:
  /** Creates a new view created from an original `view` filtered by some predication function `Func`. */
  constexpr FilterView(V view, Func func)
      : original_view_{std::move(view)}, func_{std::move(func)} {}

  /** Returns the begin iterator of FilterView. */
  constexpr auto begin() const {
    return Iterator{std::ranges::begin(original_view_), std::ranges::end(original_view_),
                    const_cast<Func*>(&func_)};
  }

  /** Returns the end sentinel of FilterView. */
  constexpr auto end() const {
    return Iterator{std::ranges::end(original_view_), std::ranges::end(original_view_),
                    const_cast<Func*>(&func_)};
  }

private:
  V original_view_;
  Func func_;
};

/**
 * An Rdd holding the data filtered from an old rdd by some function.
 * @tparam R Type of the old Rdd.
 * @tparam Func Type of the filter function.
 * The type of the old Rdd `R`'s elements should be able to invoke function `Func`,
 * the invoke result must be in boolean, and the element type shouldn't change after filter.
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
   * Main constructor of FilterRdd.
   * @param prev Reference to previous Rdd of type R
   * @param func The filter function which takes an element from prev and returns a boolean value
   */
  constexpr FilterRdd(const R& prev, Func func) : Base{prev, false} /*, func_{std::move(func)}*/ {
    static_assert(concepts::Rdd<FilterRdd<R, Func>>,
                  "Instance of FilterRdd does not satisfy Rdd concept.");
    // Create the filtered splits.
    for (const concepts::Split auto& prev_split : prev) {
      splits_.emplace_back(FilterView(prev_split, func), prev_split);
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
  std::vector<ViewSplit<FilterView<std::ranges::range_value_t<R>, Func>>> splits_{};
};

/**
 * Helper class to create FilterRdd with pipeline operator `|`.
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
 * Helper function to create FilterRdd with pipeline operator `|`.
 */
template <typename Func, concepts::Rdd R>
auto operator|(const R& r, const Filter<Func>& filter) {
  return filter(r);
}

/**
 * @example filter_even.cpp
 * Get all the even numbers from 0 to 50.
 */

/** @} */ // end of t_Filter

}  // namespace cpark

#endif  //CPARK_FILTER_RDD_H
