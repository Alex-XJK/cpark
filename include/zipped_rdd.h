#ifndef CPARK_ZIPPED_RDD_H
#define CPARK_ZIPPED_RDD_H

#include <iterator>
#include <ranges>
#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
 * An Rdd holding the zipped data from two old rdds
 * @tparam R1 Type of the first old Rdd.
 * @tparam R2 Type of the second old Rdd.
 */
template <concepts::Rdd R1, concepts::Rdd R2>
class ZippedRdd : public BaseRdd<ZippedRdd<R1, R2>> {
public:
  using Base = BaseRdd<ZippedRdd<R1, R2>>;
  friend Base;

  using V1 = utils::RddElementType<R1>;
  using V2 = utils::RddElementType<R2>;

  constexpr ZippedRdd(const R1& prev1, const R2& prev2) : Base{prev1, false} {
    static_assert(concepts::Rdd<ZippedRdd<R1, R2>>,
                  "Instance of ZippedRdd does not satisfy Rdd concept.");
    if (std::ranges::distance(prev1) != std::ranges::distance(prev2)) {
      throw std::runtime_error("R1 and R2 do not have the same number of splits.");
    }

    int cnt = 0;
    for (const concepts::Split auto& prev_split_1 : prev1) {
      auto targeted_split = prev2[cnt].begin();
      std::function<std::pair<V1, V2>(const V1&)> func = [cnt,
                                                          targeted_split](const V1& x) mutable {
        return std::make_pair(x, *(targeted_split++));
      };
      auto zippedView = prev_split_1 | std::views::transform(func);
      splits_.emplace_back(zippedView, prev_split_1);
      splits_.back().addDependency(prev_split_1);
      cnt++;
    }
  }

  constexpr ZippedRdd(const ZippedRdd&) = default;
  ZippedRdd& operator=(const ZippedRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  using ZippedViewType =
      decltype(std::declval<R1>().front() |
               std::views::transform(std::declval<std::function<std::pair<V1, V2>(const V1&)>>()));
  std::vector<ViewSplit<ZippedViewType>> splits_{};
};
}  // namespace cpark

#endif  // CPARK_TRANSFORMED_RDD_H