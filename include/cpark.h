#ifndef CPARK_CPARK_H
#define CPARK_CPARK_H

#include <any>
#include <future>
#include <ostream>
#include <queue>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "utils.h"

/** @defgroup g_creates Creations
 *  Creations generate a new cpark Rdd from program.
 */

/** @defgroup g_trans Transformations
 *  Transformations are lazy operations that define a new cpark Rdd.
 */

/** @defgroup g_acts Actions
 *  Actions launch a computation on the cpark Rdds and return a value to the program.
 */

namespace cpark {

/**
 * Configuration class for cpark.
 */
class Config {
public:
  enum class ParallelPolicy {
    Sequential,
    Thread,
#ifdef CPARK_DISTRIBUTED
    Distributed
#endif  // CPARK_DISTRIBUTED
  };

public:
  /**
   * Returns the debug name, a name that will be shown in the log message of the cpark tasks
   * created with this configuration */
  [[nodiscard]] constexpr const std::string& getDebugName() const noexcept { return debug_name_; }

  /**
   * Returns the parallel task number of the cpark task, which typically means the number of splits
   * in an Rdd.
   */
  [[nodiscard]] constexpr size_t getParallelTaskNum() const noexcept { return parallel_task_num_; }

  /**
   * Returns the parallel policy of the cpark tasks.
   */
  [[nodiscard]] constexpr ParallelPolicy getParallelPolicy() const noexcept {
    return parallel_policy_;
  }

  /** Returns a pointer to an ostream that will be used as a logger. */
  [[nodiscard]] constexpr std::ostream* getLoggerPtr() const noexcept { return logger_; }

  /**
   * Returns an ostream that will be used as a logger. If the logger is not set by user, it returns
   * an ostream that eats everything put to it.
   */
  [[nodiscard]] constexpr std::ostream& getLoggerOrNullStream() const noexcept {
    return logger_ ? *logger_ : utils::g_null_ostream;
  }

  /** Sets debug name. */
  Config& setDebugName(std::string name) noexcept {
    debug_name_ = std::move(name);
    return *this;
  }

  /** Sets parallel task number. If no parameter, default to physical supported thread number. */
  Config& setParallelTaskNum(size_t num = 0) noexcept {
    if (num == 0) {
      unsigned int n = std::thread::hardware_concurrency();
      if (n != 0) {
        parallel_task_num_ = n;
      }
    } else {
      parallel_task_num_ = num;
    }
    return *this;
  }

  /** Sets parallel policy. */
  Config& setParallelPolicy(ParallelPolicy policy) noexcept {
    parallel_policy_ = policy;
    return *this;
  }

  /** Sets logger. */
  Config& setLogger(std::ostream* logger) noexcept {
    logger_ = logger;
    return *this;
  }

private:
  std::string debug_name_{};
  size_t parallel_task_num_{8};
  ParallelPolicy parallel_policy_{ParallelPolicy::Thread};
  std::ostream* logger_{nullptr};  // Should this be here?

#ifdef CPARK_DISTRIBUTED
public:
  struct Address {
    uint32_t ip_;
    uint16_t port_;
  };

public:
  Config& addWorker(const Address& address) {
    workers_.push_back(address);
    return *this;
  }

  Config& setMaster(const Address& address) noexcept {
    master_ = address;
    return *this;
  }

private:
  std::vector<Address> workers_;
  Address master_;
#endif  //CPARK_DISTRIBUTED
};

/**
 * An execution context (or environment) for a set of cpark tasks to run. It contains the
 * information needed to evaluate the Rdd-s and run the cpark tasks, including the id information
 * of Rdd-s and Splits, the cache information, the thread synchronization information,
 * and the scheduler information.
 * Each Rdd and Split will be included in one and only one execution context.
 *
 * Users should be responsible to make sure the execution context is not out-of-lifetime when
 * executing the cpark tasks.
 * TODO: Consider whether to use smart pointers. As a fundamental library, smart pointers might
 * not be a good choice.
 */
class ExecutionContext {
public:
  /**
   * Represents a unique id for each Rdd inside this execution context.
   * Note that Rdd-s are copyable. Copied Rdd will have a same id.
   */
  using RddId = uint32_t;

  /**
   * Represents a unique id for each Split inside this execution context.
   * Note that Splits are copyable. Copied Split will have a same id.
   */
  using SplitId = uint32_t;

public:
  /** Creates execution context with default config. */
  ExecutionContext() = default;

  /** Creates execution context from a config. */
  explicit ExecutionContext(Config config) : config_{std::move(config)} {}

  /** Sets configuration. */
  void setConfig(Config config) { config_ = std::move(config); }

  /** Returns the config of the execution context. */
  const Config& getConfig() const noexcept { return config_; }

  /** Returns the next unique Rdd id. */
  RddId getAndIncRddId() { return next_rdd_id_++; }

  /** Returns the next unique Split id. */
  SplitId getAndIncSplitId() { return next_split_id_++; }

  /** Returns whether the split should be cached. */
  bool splitShouldCache(SplitId split_id) const noexcept {
    std::shared_lock guard(cache_mutex_);
    return dependent_by_.contains(split_id) && dependent_by_.at(split_id).size() >= 2;
  }

  /** Checks whether the split has already been cached. */
  bool splitCached(SplitId split_id) const noexcept {
    std::shared_lock guard(cache_mutex_);
    return cache_done_.contains(split_id) &&
           cache_done_[split_id].wait_for(std::chrono::seconds(0)) == std::future_status::ready;
  }

  void markDependency(SplitId from, SplitId to) noexcept {
    std::shared_lock guard(cache_mutex_);
    dependent_by_[to].insert(from);
  }

  /** Returns the cache for the split, if it has already been cached. */
  const std::any& getSplitCache(SplitId split_id) const {
    std::shared_lock guard(cache_mutex_);
    return cache_.at(split_id);
  }

  template <typename CacheType, typename OriginalIterator>
  std::shared_future<void> startCalculationOrGetFuture(SplitId split_id, OriginalIterator begin,
                                                       OriginalIterator end) {
    {
      std::shared_lock guard(cache_mutex_);
      if (cache_done_.contains(split_id)) {
        return cache_done_[split_id];
      }
    }

    std::promise<void> promise{};

    {
      std::unique_lock guard(cache_mutex_);
      if (cache_done_.contains(split_id)) {
        return cache_done_[split_id];
      }
      cache_done_[split_id] = promise.get_future();
    }

    CacheType cache;
    std::copy(begin, end, std::back_inserter(cache));

    {
      std::unique_lock guard(cache_mutex_);
      cache_[split_id] = std::move(cache);
      promise.set_value();
      return cache_done_[split_id];
    }
  }

private:
  Config config_{};

  // Using them to create incremental unique id for Rdd and Split.
  std::atomic<RddId> next_rdd_id_{};
  std::atomic<SplitId> next_split_id_{};

  // Which splits are relying on this one.
  std::unordered_map<SplitId, std::unordered_set<SplitId>> dependent_by_{};

  // Cache information for the Splits.
  std::unordered_map<SplitId, std::any> cache_{};
  mutable std::unordered_map<SplitId, std::shared_future<void>> cache_done_{};
  mutable std::shared_mutex cache_mutex_{};

  // Thread synchronization information.

  // Scheduler.
};

}  // namespace cpark

#endif  //CPARK_CPARK_H
