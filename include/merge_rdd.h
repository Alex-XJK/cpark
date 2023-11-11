#ifndef CPARK_MERGE_RDD_H
#define CPARK_MERGE_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
 * An Rdd holding the data merged from an old rdd by some function.
 * @tparam R Type of the old Rdd.
 */
template <concepts::Rdd R>
class MergeRdd : public BaseRdd<MergeRdd<R>> {
public:
  using Base = BaseRdd<MergeRdd<R>>;
  friend Base;

public:
  class Iterator : std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;
    using value_type = utils::RddElementType<R>;
    using split_type = BaseSplit<std::ranges::range_value_t<R>>;
    using OriginalIterator = std::ranges::iterator_t<std::ranges::range_value_t<R>>;
    using OriginalSentinel = std::ranges::sentinel_t<std::ranges::range_value_t<R>>;

    Iterator() = default;

    Iterator(std::vector<split_type>& data, size_t row = 0, size_t col = 0)
        : prev_splits_(data), row_(row), col_(col) {}

    /** Computes the current value. */
    value_type operator*() const { return *prev_splits_[row_][col_]; }

    /** Increments the iterator to the next element. */
    Iterator& operator++() {
      if (++col_ == prev_splits_[row_].size()) {
        col_ = 0;
        ++row_;
      }
      return *this;
    }

    /** Increments the iterator to the next element. */
    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    /** Two iterators equal each other if and only if they have the same position. */
    bool operator==(const Iterator& other) const { return iterator_ == other.iterator_; }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    OriginalIterator iterator_;
    std::vector<split_type> prev_splits_;
    size_t row_;
    size_t col_;
  };

  constexpr MergeRdd(const R& prev) : Base{prev, false} {
    static_assert(concepts::Rdd<MergeRdd<R>>,
                  "Instance of MergeRdd does not satisfy Rdd concept.");
    // Prepare nested splits vector
    using split_type = BaseSplit<std::ranges::range_value_t<R>>;
    std::vector<split_type> all_prev_splits;
    for (const concepts::Split auto& prev_split : prev)
      all_prev_splits.emplace_back(prev_split);

    // Create the single splits_ element
    splits_.emplace_back(
      std::ranges::subrange{
        Iterator{all_prev_splits, 0, 0},
        Iterator{all_prev_splits, all_prev_splits.size(), 0}},
      prev.front());
    for (const concepts::Split auto& prev_split : prev)
      splits_.back().addDependency(prev_split);
  }

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr MergeRdd(const MergeRdd&) = default;
  MergeRdd& operator=(const MergeRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  std::vector<ViewSplit<std::ranges::subrange<Iterator>>> splits_{};
};

/**
 * Helper class to create Union Rdd with pipeline operator `|`.
 */
class Merge {
public:
  explicit Merge() = default;

  template <concepts::Rdd R>
  auto operator()(const R& r) const {
    return MergeRdd(r);
  }
};

/**
 * Helper function to create Union Rdd with pipeline operator `|`.
 */
template <concepts::Rdd R>
auto operator|(const R& r, const Merge& merge) {
  return merge(r);
}

}  // namespace cpark

#endif  //CPARK_MERGE_RDD_H
