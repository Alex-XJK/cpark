#ifndef CPARK_ZIP_RDD_H
#define CPARK_ZIP_RDD_H

#include <tuple>
#include <iterator>
#include "base_rdd.h"
#include "utils.h"
namespace cpark {

template <std::ranges::input_range R1, std::ranges::input_range R2>
class ZipView : public std::ranges::view_interface<ZipView<R1, R2>> {
public:
  using value_type = std::tuple<std::ranges::range_value_t<R1>, std::ranges::range_value_t<R2>>;
  using iterator1 = std::ranges::iterator_t<R1>;
  using iterator2 = std::ranges::iterator_t<R2>;

  class Iterator {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = ZipView::value_type;

    Iterator(iterator1 it1, iterator2 it2) : it1_{it1}, it2_{it2} {}

    value_type operator*() const { return std::make_tuple(*it1_, *it2_); }

    Iterator& operator++() {
      ++it1_;
      ++it2_;
      return *this;
    }

    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    bool operator==(const Iterator& other) const { return it1_ == other.it1_ && it2_ == other.it2_; }
    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    iterator1 it1_;
    iterator2 it2_;
  };

  ZipView(R1 range1, R2 range2) : range1_{std::move(range1)}, range2_{std::move(range2)} {}

  auto begin() const {
    return Iterator{std::ranges::begin(range1_), std::ranges::begin(range2_)};
  }

  auto end() const {
    return Iterator{std::ranges::end(range1_), std::ranges::end(range2_)};
  }

private:
  R1 range1_;
  R2 range2_;
};

template <concepts::Rdd R1, concepts::Rdd R2>
class ZipRdd : public BaseRdd<ZipRdd<R1, R2>> {
public:
  using Base = BaseRdd<ZipRdd<R1, R2>>;
  friend Base;

  constexpr ZipRdd(const R1& prev1, const R2& prev2) : Base{prev1, false} {
    static_assert(concepts::Rdd<ZipRdd<R1, R2>>,
                  "Instance of ZipRdd does not satisfy Rdd concept.");
    
    auto it1 = prev1.begin();
    auto it2 = prev2.begin();
    while (it1 != prev1.end() && it2 != prev2.end()) {
      splits_.emplace_back(ZipView(*it1, *it2), *it1, *it2);
      splits_.back().addDependency(*it1);
      splits_.back().addDependency(*it2);
      ++it1;
      ++it2;
    }
  }

  constexpr ZipRdd(const ZipRdd&) = default;
  ZipRdd& operator=(const ZipRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }
  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  std::vector<ViewSplit<ZipView<std::ranges::range_value_t<R1>, std::ranges::range_value_t<R2>>>> splits_{};
};

/**
 * Helper class to create Transformed Rdd with pipeline operator `|`.
 */
template <concepts::Rdd R1, concepts::Rdd R2>
class Zip {
public:
  Zip() = default;

  auto operator()(const R1& r1, const R2& r2) const {
    return ZipRdd(r1, r2);
  }
};

/**
 * Helper function to create Zip Rdd with pipeline operator `|`.
 */
template <concepts::Rdd R1, concepts::Rdd R2>
auto operator|(const std::tuple<R1, R2>& r, the Zip<R1, R2>& zip) {
  return zip(std::get<0>(r), std::get<1>(r));
}

}  // namespace cpark

#endif  // CPARK_ZIP_RDD_H
