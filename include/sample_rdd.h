#ifndef CPARK_SAMPLE_RDD_H
#define CPARK_SAMPLE_RDD_H

#include <random>
#include <vector>
#include "base_rdd.h"
#include "filter_rdd.h"
#include "utils.h"
namespace cpark {

/**
 * An Rdd holding the sampled data from an old rdd by a specific sample fraction
 * @tparam R Type of the old Rdd.
 * The type of the old Rdd `R`'s elements should be able to invoke random boolean function,
 * the invoke result must be in boolean, and the element type shouldn't change after sampling.
 */
template <concepts::Rdd R>
requires std::invocable<std::function<bool(int)>, utils::RddElementType<R>>&& std::is_convertible_v<
    std::invoke_result_t<std::function<bool(int)>, utils::RddElementType<R>>, bool> class SampleRdd
    : public BaseRdd<SampleRdd<R>> {
public:
  using Base = BaseRdd<SampleRdd<R>>;
  friend Base;

public:
  /**
   * Main constructor of SampleRdd.
   * @param prev Reference to previous Rdd of type R
   * @param fraction The desired sampling ratio from the 'prev' RDD
   */
  constexpr SampleRdd(const R& prev, double fraction) : Base{prev, false}, fraction_{fraction} {
    static_assert(concepts::Rdd<SampleRdd<R>>,
                  "Instance of SampleRdd does not satisfy Rdd concept.");
    // Create the sampled splits.
    for (const concepts::Split auto& prev_split : prev) {
      splits_.emplace_back(FilterView(prev_split, func), prev_split);
      splits_.back().addDependency(prev_split);
    }
  };

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr SampleRdd(const SampleRdd&) = default;
  SampleRdd& operator=(const SampleRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  double fraction_;
  std::function<bool(int)> func = [this](int i) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::bernoulli_distribution d(fraction_);
    return d(gen);
  };
  std::vector<ViewSplit<FilterView<std::ranges::range_value_t<R>, std::function<bool(int)>>>>
      splits_{};
};

/**
 * Helper class to create Sample Rdd with pipeline operator `|`.
 */
class Sample {
public:
  explicit Sample(double fraction) : fraction_{fraction} {}
  template <concepts::Rdd R, typename T = utils::RddElementType<R>>
  requires std::invocable<std::function<bool(int)>, T>&&
      std::is_same_v<std::invoke_result_t<std::function<bool(int)>, T>, bool> auto
      operator()(const R& r) const {
    return SampleRdd(r, fraction_);
  }

private:
  double fraction_;
};

/**
 * Helper function to create Sample Rdd with pipeline operator `|`.
 */
template <concepts::Rdd R>
auto operator|(const R& r, const Sample& sample) {
  return sample(r);
}

}  // namespace cpark

#endif  //CPARK_SAMPLE_RDD_H
