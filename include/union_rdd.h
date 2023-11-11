#ifndef CPARK_UNION_RDD_H
#define CPARK_UNION_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
* An Rdd holding the union rdd.
* @tparam R Type of the old Rdd.
*/
template <concepts::Rdd R, concepts::Rdd T>
class UnionRdd : public BaseRdd<UnionRdd<R, T>> {
public:
  using Base = BaseRdd<UnionRdd<R, T>>;
  friend Base;

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
  using UnionViewtype = decltype(std::declval<R>().front());
  std::vector<ViewSplit<UnionViewtype>> splits_{};
};

///**
// * Helper class to create Union Rdd with pipeline operator `|`.
// */
//template <concepts::Rdd R>
//class Union {
//public:
//  explicit Union(R prev1) : prev1_{prev1} {}
//
//  auto operator()(const R& prev2) const {
//    return UnionRdd(prev1_, prev2);
//  }
//
//private:
//  R prev1_;
//};
//
///**
// * Helper function to create Union Rdd with pipeline operator `|`.
// */
//template <concepts::Rdd R>
//auto operator|(const R& r, const Union<R>& union_c) {
//  return union_c(r);
//}

}  // namespace cpark

#endif  //CPARK_UNION_RDD_H
