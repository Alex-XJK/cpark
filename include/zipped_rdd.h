#ifndef CPARK_ZIPPED_RDD_H
#define CPARK_ZIPPED_RDD_H

#include <vector>
#include <ranges>
#include <iterator>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

template <concepts::Rdd R1, concepts::Rdd R2>
requires std::is_convertible_v<std::ranges::range_value_t<R1>, std::ranges::range_value_t<R2>>
class ZippedRdd : public BaseRdd<ZippedRdd<R1, R2>> {
public:
  using Base = BaseRdd<ZippedRdd<R1, R2>>;
  friend Base;

  constexpr ZippedRdd(const R1& prev1, const R2& prev2) : Base{prev1, false} {
    static_assert(concepts::Rdd<ZippedRdd<R1, R2>>,
                  "Instance of ZippedRdd does not satisfy Rdd concept.");
    int cnt = 0;
    for (const concepts::Split auto& prev_split : prev1) {
      auto zippedView = prev_split | std::views::transform([&,cnt,prev2](const auto& x) {return std::make_pair(x,&prev2[cnt]);});
      splits_.emplace_back(zippedView, prev_split);
      splits_.back().addDependency(prev_split);
      cnt++;
    }
  }

  constexpr ZippedRdd(const ZippedRdd&) = default;
  ZippedRdd& operator=(const ZippedRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  //using ZippedViewType = decltype( std::declval<R1>().front() 
    //  | std::views::transform(std::declval<Func>()));
  using ZippedViewType = std::ranges::range_value_t<R1>;
  std::vector<ViewSplit<ZippedViewType>> splits_{};
};
}

#endif // CPARK_TRANSFORMED_RDD_H

