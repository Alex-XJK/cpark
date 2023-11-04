#ifndef CPARK_TRANSFORMED_RDD_H
#define CPARK_TRANSFORMED_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
* An Rdd holding the data transformed from an old rdd by some function.
* @tparam R Type of the old Rdd.
* @tparam Func Type of the transformation function.
* @tparam T Type of the data hold in this Rdd.
*/
template <concepts::Rdd R, typename Func,
          typename T = std::invoke_result_t<Func, utils::RddElementType<R>>>
requires std::invocable<Func, utils::RddElementType<R>>&& std::convertible_to<
    std::invoke_result_t<Func, utils::RddElementType<R>>, T> class TransformedRdd
    : public BaseRdd<TransformedRdd<R, Func, T>> {
public:
  using Base = BaseRdd<TransformedRdd<R, Func, T>>;
  friend Base;

  constexpr TransformedRdd(const R& prev, Func func, ExecutionContext* context) : Base{context} {
    static_assert(concepts::Rdd<TransformedRdd<R, Func, T>>,
                  "Instance of TransformedRdd does not satisfy Rdd concept.");
    // Create the transformed splits.
    for (const auto& prev_split : prev) {
      splits_.emplace_back(prev_split | std::views::transform(func), context);
    }
  }

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr TransformedRdd(const TransformedRdd&) = default;
  TransformedRdd& operator=(const TransformedRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  using TransformedViewype =
      decltype(std::declval<R>().front() | std::views::transform(std::declval<Func>()));
  std::vector<ViewSplit<TransformedViewype>> splits_{};
};

}  // namespace cpark

#endif  //CPARK_TRANSFORMED_RDD_H
