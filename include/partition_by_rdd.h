#include <concepts>
#include <ranges>

#include "base_rdd.h"
#include "filter_rdd.h"
#include "merged_view.h"
#include "utils.h"

namespace cpark {

template <concepts::Rdd R, typename Partitioner = std::hash<utils::RddElementType<R>>>
requires concepts::KeyValueRdd<R> class PartitionByRdd
    : public BaseRdd<PartitionByRdd<R, Partitioner>> {
public:
  using Base = BaseRdd<PartitionByRdd<R, Partitioner>>;
  friend Base;

public:
  explicit PartitionByRdd(const R& prev) {
    for (size_t i : std::views::iota(0u, Base::splits_num_)) {
      auto split_view =
          FilterView(MergedSameView(prev), [i, splits_num = Base::splits_num_](const auto& x) {
            return Partitioner{}(x.first) % splits_num == i;
          });
      splits_.push_back(std::move(split_view));
      for (const concepts::Split auto split : prev) {
        splits_.back().addDependency(split);
      }
    }
  }

private:
  std::vector<ViewSplit<FilterView<MergedSameView<R>, Partitioner>>> splits_;
};

}  // namespace cpark
