#ifndef CPARK_TRANSFORMED_RDD_H
#define CPARK_TRANSFORMED_RDD_H

#include <vector>

#include "rdd.h"

namespace cpark {

/**
* An Rdd holding the data transformed from an old rdd by some function.
* @tparam R Type of the old Rdd.
* @tparam Func Type of the transformation function.
* @tparam T Type of the data hold in this Rdd.
*/
template <Rdd R, typename Func,
          typename T = std::invoke_result_t<Func, std::ranges::range_value_t<R>>>
requires std::invocable<Func, std::ranges::range_value_t<R>>&& std::convertible_to<
    std::invoke_result_t<Func, std::ranges::range_value_t<R>>, T> class TransformedRdd
    : public BaseRdd<TransformedRdd<R, Func, T>> {
public:
  friend BaseRdd<TransformedRdd<R, Func, T>>;

  constexpr TransformedRdd(R prev, Func func)
      : prev_{std::move(prev)},
        func_{std::move(func)},
        split_ranges_(splits_),
        splits_{prev.splits_num()} {
    static_assert(Rdd<TransformedRdd<R, Func, T>>,
                  "Instance of TransformedRdd does not satisfy Rdd concept.");
    // Create the transformed splits.
    for (size_t i : std::views::iota(size_t{0}, splits_)) {
      split_ranges_[i] = prev_.get_split(i) | std::views::transform(func_);
    }
  }

private:
  constexpr auto get_split_impl(size_t i) const { return split_ranges_[i]; }

  constexpr size_t splits_num_impl() const { return splits_; }

public:
  constexpr auto begin() const { return std::ranges::begin(split_ranges_.front()); }

  constexpr auto end() const { return std::ranges::end(split_ranges_.back()); }

private:
  R prev_;
  Func func_;
  size_t splits_{};

  using TransformedSplitType =
      decltype(prev_.get_split(std::declval<size_t>()) | std::views::transform(func_));
  std::vector<TransformedSplitType> split_ranges_;
};

}  // namespace cpark

#endif  //CPARK_TRANSFORMED_RDD_H
