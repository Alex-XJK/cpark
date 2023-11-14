#include <concepts>
#include <memory>
#include <ranges>
#include <shared_mutex>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
 * An Rdd that performs group-by-key operation to a previous key-value typed Rdd.
 * The values that have the same key will be grouped into a sequence, and the sequence will be the
 * value of this key in the new Rdd.
 * @tparam R The previous Rdd. Must be of key-value type. Must be already partitioned.
 *           TODO: Add support for un-partition-ed Rdd-s with an extra partition.
 */
template <concepts::KeyValueRdd R>
  requires concepts::KeyValueRdd<R>
class GroupByKeyRdd : public BaseRdd<GroupByKeyRdd<R>> {
public:
  using Base = BaseRdd<GroupByKeyRdd<R>>;
  friend Base;

public:
  using KeyType = utils::RddKeyType<R>;
  using ValueType = utils::RddValueType<R>;

  /**
   * An Iterator that will calculate and store the group-by-key results in a shared state.
   */
  class Iterator : public std::forward_iterator_tag {
  public:
    using value_type =
        std::pair<KeyType, std::ranges::subrange<std::ranges::iterator_t<std::vector<ValueType>>>>;
    using difference_type = std::ptrdiff_t;

  public:
    /**
     * Creates the iterator with
     * @param original_data A subrange representing the original key-value data to be grouped.
     * @param is_end Whether this iterator is an end sentinel.
     */
    explicit Iterator(
        std::ranges::subrange<std::ranges::iterator_t<std::ranges::range_value_t<R>>> original_data,
        bool is_end)
        : original_data_{std::move(original_data)},
          inner_data_mutex_{std::make_shared<std::shared_mutex>()},
          is_end_{is_end} {}

    /**
     * Creates the iterator of empty data with
     * @param is_end Whether this iterator is an end sentinel.
     */
    explicit Iterator(bool is_end) : is_end_{is_end} {}

    Iterator() = default;

    value_type operator*() const {
      waitOrEvaluate();
      // Note that all operations to the shared data after any call of waitOrEvaluate() are read operations,
      // so mutex is no longer needed.
      checkOrInitInnerIterator();
      return {inner_iterator_->first, std::ranges::subrange(inner_iterator_->second)};
    }

    Iterator& operator++() {
      waitOrEvaluate();
      // Note that all operations to the shared data after any call of waitOrEvaluate() are read operations,
      // so mutex is no longer needed.
      checkOrInitInnerIterator();
      ++inner_iterator_;
      return *this;
    }

    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    /**
     * Two iterators are equal iff:
     * they are both end sentinels, or they have the same shared data and point to the same element.
     */
    bool operator==(const Iterator& other) const {
      if (isEnd() && other.isEnd()) {
        return true;
      }
      if (isEnd() || other.isEnd()) {
        return false;
      }
      return inner_data_ == other.inner_data_ && inner_iterator_ == other.inner_iterator_;
    }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    /** Do the actual evaluation. Must be called within the protection of inner_data_mutex_. */
    void evaluate() const {
      inner_data_ = std::make_shared<std::unordered_map<KeyType, std::vector<ValueType>>>();
      for (const auto& [key, value] : original_data_) {
        (*inner_data_)[key].push_back(value);
      }
    }

    /**
     * If the shared data is not calculated yet, calculate it or wait until it is calculated by
     * other threads.
     */
    void waitOrEvaluate() const {
      {
        std::shared_lock the_shared_lock(*inner_data_mutex_);
        if (inner_data_) {
          return;
        }
      }
      {
        std::unique_lock the_unique_lock(*inner_data_mutex_);
        if (inner_data_) {
          return;
        }
        evaluate();
      }
    }

    void checkOrInitInnerIterator() const {
      if (!inner_iterator_init_) {
        inner_iterator_ = std::ranges::begin(*inner_data_);
        inner_iterator_init_ = true;
      }
    }

    bool isEnd() const {
      return is_end_ || (inner_iterator_init_ && inner_iterator_ == std::ranges::end(*inner_data_));
    }

  private:
    // The shared state that stores the results of the group operation.
    mutable std::shared_ptr<std::unordered_map<KeyType, std::vector<ValueType>>> inner_data_{
        nullptr};
    // An iterator pointing tho the current element in the shared state.
    mutable std::ranges::iterator_t<std::unordered_map<KeyType, std::vector<ValueType>>>
        inner_iterator_{};
    // Whether `inner_iterator_` is initialized.
    mutable bool inner_iterator_init_{false};
    // The mutex to protect the shared state.
    mutable std::shared_ptr<std::shared_mutex> inner_data_mutex_{nullptr};
    std::ranges::subrange<std::ranges::iterator_t<std::ranges::range_value_t<R>>> original_data_;
    bool is_end_{false};
  };

public:
  /** Creates GroupByKeyRdd from a previous Rdd. */
  explicit GroupByKeyRdd(const R& prev) : Base{prev} {
    for (const cpark::concepts::Split auto& split : prev) {
      splits_.emplace_back(std::ranges::subrange(Iterator{split, false}, Iterator{true}),
                           Base::context_);
      splits_.back().addDependency(split);
    }
  }

  GroupByKeyRdd(const GroupByKeyRdd&) = default;
  GroupByKeyRdd& operator=(const GroupByKeyRdd&) = default;

private:
  /** Returns the iterator to the first split. */
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  /** Return the pass-by-end sentinel of the splits. */
  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  std::vector<ViewSplit<std::ranges::subrange<Iterator>>> splits_;
};

// TODO: Helper classes to be added later.

}  // namespace cpark
