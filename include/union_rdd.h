#ifndef CPARK_UNION_RDD_H
#define CPARK_UNION_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
* An Rdd holding the data union from an old rdd.
* This Rdd will hold splits of the sum of its predecessors.
* @tparam R Type of the old Rdd.
* @tparam T Type of another old Rdd.
* The elements in the two types of Rdd should be convertible.
*/
template <concepts::Rdd R, concepts::Rdd T>
requires std::is_convertible_v<std::ranges::range_value_t<R>, std::ranges::range_value_t<T>>
class UnionRdd : public BaseRdd<UnionRdd<R, T>> {
public:
  using Base = BaseRdd<UnionRdd<R, T>>;
  friend Base;

  /**
   * Main constructor of UnionRdd.
   * @param prev1 Reference to previous Rdd of type R
   * @param prev2 Reference to previous Rdd of type T
   */
  constexpr UnionRdd(const R& prev1, const T& prev2) : Base{prev1, false} {
    static_assert(concepts::Rdd<UnionRdd<R, T>>,
                  "Instance of UnionRdd does not satisfy Rdd concept.");
    // Push the splits from RDD1.
    for (const concepts::Split auto& prev_split : prev1) {
      splits_.emplace_back(prev_split, prev_split);
      splits_.back().addDependency(prev_split);
    }
    // Push the splits from RDD2.
    for (const concepts::Split auto& prev_split : prev2) {
      splits_.emplace_back(prev_split, prev_split);
      splits_.back().addDependency(prev_split);
    }
  }

  constexpr UnionRdd(const UnionRdd&) = default;
  UnionRdd& operator=(const UnionRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }
  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  using UnionViewtype = std::ranges::range_value_t<R>;
  std::vector<ViewSplit<UnionViewtype>> splits_{};
};

}  // namespace cpark

#endif  //CPARK_UNION_RDD_H
