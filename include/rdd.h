#ifndef CPARK_RDD_H
#define CPARK_RDD_H

#include <concepts>
#include <ranges>

#include "cpark.h"

namespace cpark {

/**
 * The most basic concept of RDD.
 * An Rdd is an input_range that has been logically partitioned into several sub
 * parts (split), with a member function get_split() that takes a size_t i and
 * returns its i-th split as an input_range, and a member function splits_num()
 * to get the total number of splits.
 * @tparam R Type to be checked.
 */
template <typename R>
concept Rdd = std::ranges::input_range<R> && requires(const R& r) {
  { r.get_split(std::declval<size_t>()) } -> std::ranges::input_range;
  { r.splits_num() } -> std::convertible_to<size_t>;
  requires std::same_as<std::ranges::range_value_t<decltype(r.get_split(std::declval<size_t>()))>,
                        std::ranges::range_value_t<R>>;
};

template <typename R>
concept HasGetSplitImpl = requires(const R& r) {
  { r.get_split_impl(std::declval<size_t>()) } -> std::ranges::input_range;
};

template <typename R>
concept HasSplitsNumImpl = requires(const R& r) {
  { r.splits_num_impl() } -> std::convertible_to<size_t>;
};

/**
 * A base class that holds common interfaces, operations and variables for all different Rdds.
 * This class use CRTP to achieve compile-time polymorphism to avoid the run-time cost of virtual
 * functions.
 * @tparam DerivedRdd The type of the derived Rdd.
 */
template <typename DerivedRdd>
class BaseRdd {
public:

  /**  */
  constexpr auto get_split(size_t i) const
      requires(std::derived_from<DerivedRdd, BaseRdd>&& HasGetSplitImpl<DerivedRdd>) {
    return static_cast<const DerivedRdd&>(*this).get_split_impl(i);
  }

  /** Default implementation of `get_split`. Returns nothing. */
  constexpr auto get_split(size_t i) const {}

  constexpr size_t splits_num() const
      requires(std::derived_from<DerivedRdd, BaseRdd>&& HasGetSplitImpl<DerivedRdd>) {
    return static_cast<const DerivedRdd&>(*this).splits_num_impl();
  }

  /**
   * Default implementation of `splits_num`.
   * Returns the parallel task number specified in the cpark context.
   */
  constexpr size_t splits_num() const { return context_->getParallelTaskNum(); }

protected:
  Context* context_;
};

}  // namespace cpark

#endif  // CPARK_RDD_H
