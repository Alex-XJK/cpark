#ifndef CPARK_UNION_RDD_H
#define CPARK_UNION_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
* An Rdd holding the data transformed from an old rdd by some function.
* @tparam R Type of the old Rdd.
*/
template <concepts::Rdd R>
class UnionRdd : public BaseRdd<UnionRdd<R>> {
public:
  using Base = BaseRdd<UnionRdd<R>>;
  friend Base;

  constexpr UnionRdd(const R& prev1, const R& prev2) : Base{prev1, false} {
    static_assert(concepts::Rdd<UnionRdd<R>>,
                  "Instance of UnionRdd does not satisfy Rdd concept.");
    // Create the union splits.
    for (const concepts::Split auto& prev_split : prev1) {
      splits_.emplace_back(prev_split, prev_split);
      splits_.back().addDependency(prev_split);
    }
    for (const concepts::Split auto& prev_split : prev2) {
      splits_.emplace_back(prev_split, prev_split);
      splits_.back().addDependency(prev_split);
    }
  }

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr UnionRdd(const UnionRdd&) = default;
  UnionRdd& operator=(const UnionRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  using UnionViewtype = decltype(std::declval<R>().front());
  // A vector holding the splits for this rdd.
  std::vector<ViewSplit<UnionViewtype>> splits_{};
};

/**
 * Helper class to create Union Rdd with pipeline operator `|`.
 */
template <concepts::Rdd R>
class Union {
public:
  explicit Union(R prev1) : prev1_{prev1} {}

  auto operator()(const R& prev2) const {
    return UnionRdd(prev1_, prev2);
  }

private:
  R prev1_;
};

/**
 * Helper function to create Union Rdd with pipeline operator `|`.
 */
template <concepts::Rdd R>
auto operator|(const R& r, const Union<R>& union_c) {
  return union_c(r);
}

}  // namespace cpark

#endif  //CPARK_UNION_RDD_H
