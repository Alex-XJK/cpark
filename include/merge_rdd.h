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
  class Iterator : std::random_access_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::random_access_iterator_tag;
    using value_type = utils::RddElementType<R>;
    using OriginalIterator = std::ranges::iterator_t<std::ranges::range_value_t<R>>;
    using OriginalSentinel = std::ranges::sentinel_t<std::ranges::range_value_t<R>>;

    Iterator() = default;

    Iterator(std::vector<OriginalIterator> begins, std::vector<OriginalIterator> ends, bool isEnd = false)
        : begins_(begins), ends_(ends), isEnd_(isEnd) {
      if (!isEnd_) {
        row_ = 0;
        current_ = begins_.size() > 0 ? begins_[row_] : OriginalIterator();
      }
      else {
        row_ = ends_.size() > 0 ? ends_.size() - 1 : 0;
        current_ = ends_.size() > 0 ? ends_[row_] : OriginalIterator();
      }
    }

    /** Computes the current value. */
    value_type operator*() const { return *current_; }

    /** Arrow operator. */
    value_type* operator->() const { return &(*current_); }

    /** Increments the iterator to the next element. */
    Iterator& operator++() {
      ++current_;
      while (current_ == ends_[row_] && row_ + 1 < begins_.size()) {
        ++row_;
        current_ = begins_[row_];
      }
      return *this;
    }

    /** Increments the iterator to the next element. */
    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    /** Compound addition operator. */
    Iterator& operator+=(difference_type n) {
      if (n >= 0)
        while (n > 0) {
          ++(*this);
          --n;
        }
      else
        while (n < 0) {
          --(*this);
          ++n;
        }
      return *this;
    }

    /** Compound subtraction operator. */
    Iterator& operator-=(difference_type n) {
      return *this += -n;
    }

    /** Addition operator. */
    Iterator operator+(difference_type n) const {
      Iterator temp = *this;
      temp += n;
      return temp;
    }

    /** Subtraction operator. */
    Iterator operator-(difference_type n) const {
      Iterator temp = *this;
      temp -= n;
      return temp;
    }

    /** Subscript operator. */
    value_type& operator[](difference_type n) const {
      return *(*this + n);
    }

    /** Equality operator. */
    bool operator==(const Iterator& other) const { return current_ == other.current_; }

    /** Inequality operator. */
    bool operator!=(const Iterator& other) const { return !(*this == other); }

    /** Less than operator. */
    bool operator<(const Iterator& other) const {
      return current_ < other.current_;
    }

    /** Greater than operator. */
    bool operator>(const Iterator& other) const {
      return current_ > other.current_;
    }

    /** Less than or equal to operator. */
    bool operator<=(const Iterator& other) const {
      return current_ <= other.current_;
    }

    /** Greater than or equal to operator. */
    bool operator>=(const Iterator& other) const {
      return current_ >= other.current_;
    }

    /** Difference operator. */
    difference_type operator-(const Iterator& other) const {
      return current_ - other.current_;
    }

  private:
    std::vector<OriginalIterator> begins_;
    std::vector<OriginalIterator> ends_;
    OriginalIterator current_;
    bool isEnd_;
    size_t row_ = 0;
  };

  constexpr MergeRdd(const R& prev) : Base{prev, false} {
    static_assert(concepts::Rdd<MergeRdd<R>>,
                  "Instance of MergeRdd does not satisfy Rdd concept.");
    // Prepare nested splits vector
    using OriginalIterator = std::ranges::iterator_t<std::ranges::range_value_t<R>>;
    std::vector<OriginalIterator> all_prev_splits_begins;
    std::vector<OriginalIterator> all_prev_splits_ends;
    for (const concepts::Split auto& prev_split : prev) {
      all_prev_splits_begins.emplace_back(std::ranges::begin(prev_split));
      all_prev_splits_ends.emplace_back(std::ranges::end(prev_split));
    }

    // Create the single splits_ element
    splits_.emplace_back(
      std::ranges::subrange{
        Iterator{all_prev_splits_begins, all_prev_splits_ends, false},
        Iterator{all_prev_splits_begins, all_prev_splits_ends, true},
      },
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
