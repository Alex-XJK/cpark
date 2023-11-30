#ifndef CPARK_PARTITION_BY_RDD_H
#define CPARK_PARTITION_BY_RDD_H

#include <concepts>
#include <ranges>

#include "base_rdd.h"
#include "filter_rdd.h"
#include "merged_view.h"
#include "utils.h"

namespace cpark {

/**
 * Re-partition an Rdd of key-value pairs using a partitioner function into new splits.
 * @tparam R Original Rdd. Its elements must be of key-value type (pair-like).
 * @tparam Partitioner A partitioner function mapping the key-type to a size_t. Each element in the
 *                     Rdd whose key is mapped to `i` will be assigned to the `i % split_num`-th
 *                     split in the new Rdd.
 */
template <concepts::KeyValueRdd R, typename Partitioner = std::hash<utils::RddKeyType<R>>>
class PartitionByRdd : public BaseRdd<PartitionByRdd<R, Partitioner>> {
public:
  using Base = BaseRdd<PartitionByRdd<R, Partitioner>>;
  friend Base;

public:
  /**
   * Creates a PartitionByRdd based on an old Rdd `prev`, using the partition function `partitioner`.
   */
  PartitionByRdd(const R& prev, Partitioner partitioner)
      : Base{prev}, partitioner_{std::move(partitioner)} {
    for (size_t i : std::views::iota(0u, Base::splits_num_)) {
      auto split_view =
          FilterView(MergedSameView(prev), PartitionerHelper{i, Base::splits_num_, &partitioner_});
      splits_.emplace_back(std::move(split_view), Base::context_);
      for (const concepts::Split auto split : prev) {
        splits_.back().addDependency(split);
      }
    }
  }

  /**
   * Creates a PartitionByRdd based on an old Rdd `prev`, using the default partitioner function
   * std::hash{}.
   */
  explicit PartitionByRdd(const R& prev) : PartitionByRdd{prev, Partitioner{}} {}

  /** Add default constructors to avoid some liter errors. */
  PartitionByRdd(const PartitionByRdd&) = default;
  PartitionByRdd& operator=(const PartitionByRdd&) = default;

private:
  /** Returns the iterator to the first split. */
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  /** Return the pass-by-end sentinel of the splits. */
  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  /**
   * A helper callable object that helps to integrate the partitioner function with FilterView.
   * Each split will hold one instance of this class to check whether an element belongs to this split.
   */
  class PartitionerHelper {
  public:
    /**
     * Constructor.
     * @param split_index Index of the current split.
     * @param splits_num Number of total splits.
     * @param partitioner The partitioner.
     */
    PartitionerHelper(size_t split_index, size_t splits_num, Partitioner* partitioner)
        : split_index_{split_index}, splits_num_{splits_num}, partitioner_{partitioner} {}

    /**
     * Checks whether an Rdd element belongs to a split.
     */
    bool operator()(const utils::RddElementType<R>& x) const {
      return (*partitioner_)(x.first) % splits_num_ == split_index_;
    }

  private:
    size_t split_index_, splits_num_;
    Partitioner* partitioner_;
  };

private:
  std::vector<ViewSplit<FilterView<MergedSameView<R>, PartitionerHelper>>> splits_;
  Partitioner partitioner_;
};

template <typename... Args>
class PartitionBy;

/**
 * A helper class to create partition by rdd with user specified partitioner.
 */
template <typename Partitioner>
class PartitionBy<Partitioner> {
public:
  explicit PartitionBy(Partitioner partitioner) : partitioner_{std::move(partitioner)} {}

  template <concepts::Rdd R, typename K = utils::RddKeyType<R>>
    requires std::invocable<Partitioner, K> &&
             std::convertible_to<std::invoke_result_t<Partitioner, K>, size_t>
  auto operator()(const R& r) const {
    return PartitionByRdd(r, partitioner_);
  }

private:
  Partitioner partitioner_;
};

/**
 * A helper class to create partition by rdd with default partitioner (std::hash).
 */
template <>
class PartitionBy<> {
public:
  template <concepts::Rdd R>
  auto operator()(const R& r) const {
    return PartitionByRdd(r);
  }
};

/** Pipeline operator helper. */
template <cpark::concepts::Rdd R, typename P>
auto operator|(const R& r, const P& p) {
  return p(r);
}

}  // namespace cpark

#endif  // CPARK_PARTITION_BY_RDD_H