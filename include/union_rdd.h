#ifndef CPARK_UNION_RDD_H
#define CPARK_UNION_RDD_H

#include <vector>

#include "base_rdd.h"
#include "cassert"
#include "merged_view.h"
#include "utils.h"

namespace cpark {

/** @defgroup t_Union The Union Transformation
 *  This forms the Union Transformation of our cpark library
 *  @image html union.drawio.png "UnionRdd diagram" width=50%
 *  @see UnionRdd
 *  @{
 */

/**
* An Rdd holding the data union from an old rdd.
* This Rdd will hold splits of the sum of its predecessors.
* @tparam R1 Type of the old Rdd.
* @tparam R2 Type of another old Rdd.
* The elements in the two types of Rdd should be convertible.
*/
template <concepts::Rdd R1, concepts::Rdd R2>
requires std::is_same_v<utils::RddElementType<R1>, utils::RddElementType<R2>> class UnionRdd
    : public BaseRdd<UnionRdd<R1, R2>> {
public:
  using Base = BaseRdd<UnionRdd<R1, R2>>;
  friend Base;

  /**
   * Main constructor of UnionRdd.
   * @param prev1 Reference to previous Rdd of type R1
   * @param prev2 Reference to previous Rdd of type R2
   */
  constexpr UnionRdd(const R1& prev1, const R2& prev2) : Base{prev1, false} {
    static_assert(concepts::Rdd<UnionRdd<R1, R2>>,
                  "Instance of UnionRdd does not satisfy Rdd concept.");
    auto empty_split_1 = std::ranges::subrange{prev1.front().end(), prev1.front().end()};
    auto empty_split_2 = std::ranges::subrange{prev2.front().end(), prev2.front().end()};

    // Push the splits from RDD1.
    for (const concepts::Split auto& prev_split : prev1) {
      splits_.emplace_back(MergedTwoDiffView(std::ranges::subrange(prev_split), empty_split_2),
                           prev_split);
      splits_.back().addDependency(prev_split);
    }
    // Push the splits from RDD2.
    for (const concepts::Split auto& prev_split : prev2) {
      splits_.emplace_back(MergedTwoDiffView(empty_split_1, std::ranges::subrange(prev_split)),
                           prev_split);
      splits_.back().addDependency(prev_split);
    }
  }

  constexpr UnionRdd(const UnionRdd&) = default;
  UnionRdd& operator=(const UnionRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }
  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  using SplitType1 = std::ranges::range_value_t<R1>;
  using SplitType2 = std::ranges::range_value_t<R2>;
  using SubrangeSplitType1 = std::ranges::subrange<std::ranges::iterator_t<SplitType1>>;
  using SubrangeSplitType2 = std::ranges::subrange<std::ranges::iterator_t<SplitType2>>;
  using UnionViewType = MergedTwoDiffView<SubrangeSplitType1, SubrangeSplitType2>;
  std::vector<ViewSplit<UnionViewType>> splits_;
};

/** @} */ // end of t_Union

}  // namespace cpark

#endif  //CPARK_UNION_RDD_H
