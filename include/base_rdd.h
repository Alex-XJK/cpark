#ifndef CPARK_BASE_RDD_H
#define CPARK_BASE_RDD_H

#include <concepts>
#include <ranges>
#include <variant>

#include "cpark.h"

namespace cpark {

namespace concepts {

/**
 * A concept that requires its classes' objects are id-able, which means these objects can be
 * identified by a numerical id.
 * These classes should have an `id()` member function returning a numerical id.
 */
template <typename T>
concept HasId = requires(const T& t) {
  t.id();
  requires std::is_arithmetic_v<decltype(t.id())>;
};

/**
 * A concept that requires its objects have some dependency relationship between each other.
 * A `HasDependency` must be `HasId`, with the following member functions:
 * dependencies() returns a list of the id-s of the dependencies of the current object.
 * addDependency() adds the id of a new dependency to the current object.
 */
template <typename T>
concept HasDependency = concepts::HasId<T> && requires(const T& t) {
  { t.dependencies() } -> std::ranges::input_range;
  t.addDependency(t.id());
};

/**
 * Split is a sub-partition of data from an Rdd, and is the smallest computing unit in cpark's
 * computing tasks,
 * It is an input_range whose objects have some dependency relationship between each other.
 */
template <typename S>
concept Split = std::ranges::input_range<S> && concepts::HasDependency<S>;

/**
 * The most basic concept of RDD.
 * An Rdd is an abstraction of a data set that has been logically partitioned into several sub
 * parts (split).
 * An Rdd should be an input_range of the splits.
 */
template <typename R>
concept Rdd = std::ranges::input_range<R> && concepts::Split<std::ranges::range_value_t<R>>;

/**
 * Concept of classes that have a member function `beginImpl()` returning an input_iterator.
 */
template <typename R>
concept HasBeginImpl = requires(const R& r) {
  { r.beginImpl() } -> std::input_iterator;
};

/**
 * Concept of classes that have a member function `endImpl()` returning an iterator sentinel.
 */
template <typename R>
concept HasEndImpl = requires(const R& r) {
  { r.endImpl() } -> std::sentinel_for<decltype(r.beginImpl())>;
};

}  // namespace concepts

/**
 * A base Split class containing common data and interfaces for different Splits.
 * This class uses CRTP to achieve compile-time polymorphism to avoid the run-time cost of virtual
 * functions.
 * @tparam DerivedSplit The type of the derived split. Derived split class should implement these
 * functions: beginImpl(), endImpl().
 */
template <typename DerivedSplit>
class BaseSplit : public std::ranges::view_interface<DerivedSplit> {
public:
  /** Initialize the split with `context` and assign a unique split id to it. */
  explicit BaseSplit<DerivedSplit>(ExecutionContext* context)
      : context_{context}, split_id_{context_->getAndIncSplitId()} {}

  /**
   * Returns the iterator pointing to the first element in the Split.
   * This function is only an interface.
   * The execution of this function is totally delegated to DerivedSplit::beginImpl().
   */
  auto begin() const requires concepts::HasBeginImpl<DerivedSplit> {
    return static_cast<const DerivedSplit&>(*this).beginImpl();
  }

  /**
   * Returns an iterator sentinel that marks the end of the split's element iterator.
   * This function is only an interface.
   * The execution of this function is totally delegated to DerivedSplit::endImpl().
   */
  auto end() const requires concepts::HasEndImpl<DerivedSplit> {
    return static_cast<const DerivedSplit&>(*this).endImpl();
  }

  /**
   * Returns a range that contains the split id-s of all the *direct* dependency of the current
   * split.
   */
  auto dependencies() const noexcept {
    return std::ranges::subrange(std::ranges::begin(dependencies_),
                                 std::ranges::end(dependencies_));
  }

  /**
   * Adds `split_id` to the dependencies of the current split.
   */
  void addDependency(ExecutionContext::SplitId split_id) { dependencies_.push_back(split_id); }

  /**
   * Adds `split`'s id to the dependencies of the current split.
   */
  template <typename T>
  void addDependency(const BaseSplit<T>& split) {
    addDependency(split.split_id_);
  }

  auto id() const noexcept { return split_id_; }

protected:
  ExecutionContext* const context_{};
  const ExecutionContext::SplitId split_id_{};
  std::vector<ExecutionContext::SplitId> dependencies_{};
};

/**
 * A general cached split class, who will either read the data by using the
 * iterator from DerivedSplit, or read the data from the execution context's cache, depending on the
 * caching information from the execution context.
 * @tparam DerivedSplit The original split to be added with a cache.
 */
template <typename DerivedSplit>
class CachedSplit : public BaseSplit<CachedSplit<DerivedSplit>> {
public:
  using Base = BaseSplit<CachedSplit<DerivedSplit>>;
  friend Base;
  using CacheType = std::vector<std::ranges::range_value_t<DerivedSplit>>;

  /**
   * A special kind of iterator, who will possibly read the data by using the iterator from
   * DerivedSplit, or read the data from the execution context's cache.
   */
  class Iterator : std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = std::ranges::range_value_t<DerivedSplit>;
    using CacheIterator = std::ranges::iterator_t<CacheType>;
    using OriginalIterator = std::ranges::iterator_t<DerivedSplit>;

    Iterator() = default;

    template <typename Iter>
        explicit Iterator(Iter iterator) requires std::same_as<Iter, CacheIterator> ||
        std::same_as<Iter, OriginalIterator> : iterator_{iterator} {}

    value_type operator*() const {
      if (std::holds_alternative<CacheIterator>(iterator_)) [[unlikely]] {
        return *std::get<CacheIterator>(iterator_);
      } else {
        return *std::get<OriginalIterator>(iterator_);
      }
    }

    Iterator& operator++() {
      if (std::holds_alternative<CacheIterator>(iterator_)) [[unlikely]] {
        ++std::get<CacheIterator>(iterator_);
      } else {
        ++std::get<OriginalIterator>(iterator_);
      }
      return *this;
    }

    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    bool operator==(const Iterator& other) const {
      if (std::holds_alternative<CacheIterator>(iterator_) &&
          std::holds_alternative<CacheIterator>(other.iterator_)) [[unlikely]] {
        return std::get<CacheIterator>(iterator_) == std::get<CacheIterator>(other.iterator_);
      } else if (std::holds_alternative<OriginalIterator>(iterator_) &&
                 std::holds_alternative<OriginalIterator>(other.iterator_)) {
        return std::get<OriginalIterator>(iterator_) == std::get<OriginalIterator>(other.iterator_);
      } else [[unlikely]] {
        return false;
      }
    }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    // A variant holds either an iterator from the original split, or an iterator of the cache.
    std::variant<CacheIterator, OriginalIterator> iterator_;
  };

public:
  explicit CachedSplit(ExecutionContext* context) : BaseSplit<CachedSplit<DerivedSplit>>{context} {}

private:
  bool shouldCache() const noexcept { return Base::context_->splitShouldCache(Base::split_id_); }

  bool hasCached() const noexcept { return Base::context_->splitCached(Base::split_id_); }

  const CacheType& getCache() const {
    return std::any_cast<const CacheType&>(Base::context_->getSplitCache(Base::split_id_));
  }

  /** Returns the iterator pointing to the first element in the Split. */
  auto beginImpl() const requires concepts::HasBeginImpl<DerivedSplit> {
    if (shouldCache() && hasCached()) {
      return Iterator{std::ranges::begin(getCache())};
    } else {
      return Iterator{static_cast<const DerivedSplit&>(*this).beginImpl()};
    }
  }

  /** Returns an iterator sentinel that marks the end of the split's element iterator. */
  auto endImpl() const requires concepts::HasEndImpl<DerivedSplit> {
    if (shouldCache() && hasCached()) {
      return Iterator{std::ranges::end(getCache())};
    } else {
      return Iterator{static_cast<const DerivedSplit&>(*this).endImpl()};
    }
  }
};

template <std::ranges::view V>
class ViewSplit : public CachedSplit<ViewSplit<V>> {
public:
  ViewSplit(V view, ExecutionContext* context)
      : CachedSplit<ViewSplit<V>>{context}, view_{std::move(view)} {}

  auto beginImpl() const { return std::ranges::begin(view_); }

  auto endImpl() const { return std::ranges::end(view_); }

private:
  V view_;
};

/**
 * A base class that holds common interfaces, operations and variables for all different Rdd-s.
 * This class uses CRTP to achieve compile-time polymorphism to avoid the run-time cost of virtual
 * functions.
 * @tparam DerivedRdd The type of the derived Rdd. Derived Rdd class should implement these
 * functions: beginImpl(), endImpl().
 */
template <typename DerivedRdd>
class BaseRdd : public std::ranges::view_interface<BaseRdd<DerivedRdd>> {
public:
  BaseRdd() = default;

  /**
   * Creates a BaseRdd with a previous BaseRdd.
   * The new BaseRdd will have the same execution context and split num with the previous one,
   * but will be assigned another unique rdd_id.
   */
  template <concepts::Rdd R>
  explicit BaseRdd(const BaseRdd<R>& prev)
      : context_{prev.context_},
        rdd_id_{prev.context_->getAndIncRddId()},
        splits_num_{prev.splits_num_} {}

  /**
   * Creates a new BaseRdd with an execution context.
   * The new BaseRdd will have this execution context and have the same number of splits as
   * configured in the execution context.
   * It will be assigned a unique rdd_id.
   */
  explicit BaseRdd(ExecutionContext* context)
      : context_{context},
        rdd_id_{context_->getAndIncRddId()},
        splits_num_{context_->getConfig().getParallelTaskNum()} {}

  /**
   * Returns an forward Iterator that points to the first split contained in this Rdd.
   */
  auto begin() const requires concepts::HasBeginImpl<DerivedRdd> {
    return static_cast<const DerivedRdd&>(*this).beginImpl();
  }

  /**
   * Returns an iterator sentinel which marks the end for the iterators pointing to the splits in
   * this Rdd.
   */
  auto end() const requires concepts::HasEndImpl<DerivedRdd> {
    return static_cast<const DerivedRdd&>(*this).endImpl();
  }

  /** Returns the rdd id. */
  auto id() const noexcept { return rdd_id_; }

protected:
  ExecutionContext* const context_{};
  const ExecutionContext::RddId rdd_id_{};
  size_t splits_num_{};
};

}  // namespace cpark

#endif  // CPARK_BASE_RDD_H
