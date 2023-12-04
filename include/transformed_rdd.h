#ifndef CPARK_TRANSFORMED_RDD_H
#define CPARK_TRANSFORMED_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/** @ingroup g_trans
 *  @defgroup t_Transform Map(Transform)
 *  This forms the Map(Transform) Transformation of our cpark library
 *  @image html transform.drawio.png "TransformedRdd diagram" width=50%
 *  @see TransformedRdd
 *  @see Transform
 *  @{
 */

/**
* An Rdd holding the data transformed from an old rdd by some function.
* @tparam R Type of the old Rdd.
* @tparam Func Type of the transformation function.
* @tparam T Type of the data hold in this Rdd.
* The type of the old Rdd `R`'s elements should be able to invoke function `Func`,
* and the result type should be able to convert to type `T`.
*/
template <concepts::Rdd R, typename Func,
          typename T = std::invoke_result_t<Func, utils::RddElementType<R>>>
requires std::invocable<Func, utils::RddElementType<R>>&& std::convertible_to<
    std::invoke_result_t<Func, utils::RddElementType<R>>, T> class TransformedRdd
    : public BaseRdd<TransformedRdd<R, Func, T>> {
public:
  using Base = BaseRdd<TransformedRdd<R, Func, T>>;
  friend Base;

  constexpr TransformedRdd(const R& prev, Func func) : Base{prev, false} {
    static_assert(concepts::Rdd<TransformedRdd<R, Func, T>>,
                  "Instance of TransformedRdd does not satisfy Rdd concept.");
    // Create the transformed splits.
    for (const concepts::Split auto& prev_split : prev) {
      splits_.emplace_back(prev_split | std::views::transform(func), prev_split);
      splits_.back().addDependency(prev_split);
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
  // A vector holding the splits for this rdd.
  std::vector<ViewSplit<TransformedViewype>> splits_{};
};

/**
 * Helper class to create Transformed Rdd with pipeline operator `|`.
 */
template <typename Func>
class Transform {
public:
  explicit Transform(Func func) : func_{std::move(func)} {}

  template <concepts::Rdd R, typename T = utils::RddElementType<R>,
            typename U = std::invoke_result_t<Func, T>>
  requires std::invocable<Func, T>&& std::convertible_to<std::invoke_result_t<Func, T>, U> auto
  operator()(const R& r) const {
    return TransformedRdd(r, func_);
  }

private:
  Func func_;
};

/**
 * Helper function to create Transformed Rdd with pipeline operator `|`.
 */
template <typename Func, concepts::Rdd R>
auto operator|(const R& r, const Transform<Func>& transform) {
  return transform(r);
}

/**
 * @example simple.cpp
 * This is an simple use case of cpark basic operations.
 */

/**
 * @example speed_check.cpp
 * This is an example program to compare
 * the running speed and code complexity difference
 * between C++ standard ranges operations and
 * cpark operations with different execution cores.
 */

/** @} */ // end of t_Transform

}  // namespace cpark

#endif  //CPARK_TRANSFORMED_RDD_H
