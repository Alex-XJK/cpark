#include <future>
#include <ranges>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cpark.h"
#include "gtest/gtest.h"

using std::operator"" s;
using cpark::Config;
using cpark::ExecutionContext;

class ExecutionContextTest : public ::testing::Test {
protected:
  void SetUp() override {
    const auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    test_config_.setDebugName("Config for test suite: "s + test_info->test_suite_name() +
                              ", test case: " + test_info->test_case_name());
  }

  void TearDown() override { test_config_ = Config{}; }

protected:
  Config test_config_;
  ExecutionContext test_context_{test_config_};
};

TEST_F(ExecutionContextTest, Config) {
  Config test_config;
  test_config.setLogger(&std::cout);
  test_config.setParallelPolicy(Config::ParallelPolicy::Sequential);
  test_config.setDebugName("test debug name");
  test_config.setParallelTaskNum(114536);
  test_context_.setConfig(test_config);
  EXPECT_EQ(test_context_.getConfig().getLoggerPtr(), test_config.getLoggerPtr());
  EXPECT_EQ(test_context_.getConfig().getParallelPolicy(), test_config.getParallelPolicy());
  EXPECT_EQ(test_context_.getConfig().getDebugName(), test_config.getDebugName());
  EXPECT_EQ(test_context_.getConfig().getParallelTaskNum(), test_config.getParallelTaskNum());
}

TEST_F(ExecutionContextTest, RddId) {
  // Concurrently call getAndIncRddId, and make sure the generated rdd id-s are unique.

  std::set<ExecutionContext::RddId> generated_ids;
  std::mutex mutex;

  const size_t test_task_num = 32;
  std::vector<std::future<void>> futures;
  for (size_t i : std::views::iota(0u, test_task_num)) {
    futures.emplace_back(std::async([&]() {
      auto rdd_id = test_context_.getAndIncRddId();
      {
        std::unique_lock lock(mutex);
        generated_ids.insert(rdd_id);
      }
    }));
  }

  for (const auto& future : futures) {
    future.wait();
  }

  EXPECT_EQ(generated_ids.size(), test_task_num);
}

TEST_F(ExecutionContextTest, SplitId) {
  // Concurrently call getAndIncSplitId, and make sure the generated split id-s are unique.

  std::set<ExecutionContext::SplitId> generated_ids;
  std::mutex mutex;

  const size_t test_task_num = 32;
  std::vector<std::future<void>> futures;
  for (size_t i : std::views::iota(0u, test_task_num)) {
    futures.emplace_back(std::async([&]() {
      auto split_id = test_context_.getAndIncSplitId();
      {
        std::unique_lock lock(mutex);
        generated_ids.insert(split_id);
      }
    }));
  }

  for (const auto& future : futures) {
    future.wait();
  }

  EXPECT_EQ(generated_ids.size(), test_task_num);
}

// TODO: tests for cache.
