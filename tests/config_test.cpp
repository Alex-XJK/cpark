#include <string>
#include <thread>

#include "cpark.h"
#include "gtest/gtest.h"
#include "utils.h"

using std::operator"" s;
using cpark::Config;

class ConfigTest : public ::testing::Test {
protected:
  void SetUp() override {
    const auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    test_config_.setDebugName("Config for test suite: "s + test_info->test_suite_name() +
                              ", test case: " + test_info->test_case_name());
  }

  void TearDown() override { test_config_ = Config{}; }

protected:
  Config test_config_;
};

TEST_F(ConfigTest, DebugName) {
  const auto test_debug_name = "test debug name"s;
  test_config_.setDebugName(test_debug_name);
  EXPECT_EQ(test_config_.getDebugName(), test_debug_name);
}

TEST_F(ConfigTest, ParallelTaskNum) {
  const auto real_hardware_concurrency = std::thread::hardware_concurrency();
  test_config_.setParallelTaskNum();
  EXPECT_EQ(test_config_.getParallelTaskNum(), real_hardware_concurrency);

  const size_t test_parallel_task_num = 16;
  test_config_.setParallelTaskNum(test_parallel_task_num);
  EXPECT_EQ(test_config_.getParallelTaskNum(), test_parallel_task_num);
}

TEST_F(ConfigTest, ParallelPolicy) {
  const auto test_parallel_policy = Config::ParallelPolicy::Sequential;
  test_config_.setParallelPolicy(test_parallel_policy);
  EXPECT_EQ(test_config_.getParallelPolicy(), test_parallel_policy);
}

TEST_F(ConfigTest, Logger) {
  test_config_.setLogger(nullptr);
  EXPECT_EQ(&test_config_.getLoggerOrNullStream(), &cpark::utils::g_null_ostream);

  std::ostringstream test_ostream;
  test_config_.setLogger(&test_ostream);
  EXPECT_EQ(test_config_.getLoggerPtr(), &test_ostream);
  const auto test_log_message = "test log message";
  test_config_.getLoggerOrNullStream() << test_log_message;
  EXPECT_TRUE(test_ostream.str().find(test_log_message) != std::string::npos);
}
