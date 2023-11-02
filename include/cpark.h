#ifndef CPARK_CPARK_H
#define CPARK_CPARK_H

#include <string>
#include <ostream>
#include <vector>

#include "utils.h"

namespace cpark {

class Context {
public:
  enum class ParallelPolicy {
    Sequential,
    Thread,
#ifdef CPARK_DISTRIBUTED
    Distributed
#endif // CPARK_DISTRIBUTED
  };

public:
  [[nodiscard]] constexpr const std::string& getDebugName() const noexcept {
    return debug_name_;
  }

  [[nodiscard]] constexpr size_t getParallelTaskNum() const noexcept {
    return parallel_task_num_;
  }

  [[nodiscard]] constexpr ParallelPolicy getParallelPolicy() const noexcept {
    return parallel_policy_;
  }

  [[nodiscard]] constexpr std::ostream& getLogger() const {
    if (!logger_) {
      throw;
    }
    return *logger_;
  }

  Context& setDebugName(std::string name) noexcept {
    debug_name_ = std::move(name);
    return *this;
  }

  Context& setParallelTaskNum(size_t num) noexcept {
    parallel_task_num_ = num;
    return *this;
  }

  Context& setParallelPolicy(ParallelPolicy policy) noexcept {
    parallel_policy_ = policy;
    return *this;
  }

  Context& setLogger(std::ostream* logger) noexcept {
    logger_ = logger;
    return *this;
  }

private:
  std::string debug_name_ {};
  size_t parallel_task_num_ {8};
  ParallelPolicy parallel_policy_ {ParallelPolicy::Thread};
  std::ostream* logger_ {&utils::g_null_stream};

#ifdef CPARK_DISTRIBUTED
public:
  struct Address {
    uint32_t ip_;
    uint16_t port_;
  };

public:
  Context& addWorker(const Address& address) {
    workers_.push_back(address);
    return *this;
  }

  Context& setMaster(const Address& address) noexcept {
    master_ = address;
    return *this;
  }
private:
  std::vector<Address> workers_;
  Address master_;
#endif //CPARK_DISTRIBUTED
};

} // namespace cpark

#endif //CPARK_CPARK_H
