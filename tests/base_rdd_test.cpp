#include <future>
#include <random>
#include <ranges>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cpark.h"
#include "gtest/gtest.h"
#include "plain_rdd.h"

using std::operator"" s;
using cpark::BaseRdd;
using cpark::BaseSplit;
using cpark::CachedSplit;
using cpark::Config;
using cpark::ExecutionContext;
using cpark::PlainRdd;

class BaseRddTest : public ::testing::Test {
protected:
  void SetUp() override {
    const auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    test_config_
        .setDebugName("Config for test suite: "s + test_info->test_suite_name() +
                      ", test case: " + test_info->test_case_name())
        .setParallelTaskNum(test_parallel_task_num_)
        .setLogger(&std::cerr)
        .setParallelPolicy(Config::ParallelPolicy::Thread);
    test_context_.setConfig(test_config_);

    // Fill the test_original_data_ with some random test data.
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> distribution(std::numeric_limits<int>::min(),
                                                    std::numeric_limits<int>::max());
    auto random_test_data = std::views::iota(0u, test_data_size_) |
                            std::views::transform([&](auto x) { return distribution(gen); });
    std::ranges::copy(random_test_data, std::back_inserter(test_original_data_));

    // Create test rdd.
    test_plain_rdd_ = std::make_unique<PlainRdd<std::ranges::subrange<std::vector<int>::iterator>>>(
        std::ranges::subrange(test_original_data_), &test_context_);
  }

protected:
  Config test_config_;
  ExecutionContext test_context_{test_config_};
  size_t test_parallel_task_num_{8};
  size_t test_data_size_{test_parallel_task_num_ * 16};
  std::vector<int> test_original_data_;
  // Use unique_ptr to avoid constructing the object at the beginning for convenience.
  std::unique_ptr<PlainRdd<std::ranges::subrange<std::vector<int>::iterator>>> test_plain_rdd_;
};

TEST_F(BaseRddTest, RddBasicOperations) {
  EXPECT_FALSE(test_plain_rdd_->empty());
  EXPECT_TRUE(*test_plain_rdd_);
  EXPECT_EQ(test_plain_rdd_->size(), test_config_.getParallelTaskNum());
  EXPECT_EQ((*test_plain_rdd_)[0].id(), test_plain_rdd_->front().id());
  EXPECT_EQ((*test_plain_rdd_)[test_plain_rdd_->size() - 1].id(), test_plain_rdd_->back().id());
  EXPECT_EQ(test_plain_rdd_->begin()->id(), test_plain_rdd_->front().id());
  EXPECT_EQ(test_plain_rdd_->begin() + test_plain_rdd_->size(), test_plain_rdd_->end());
}

TEST_F(BaseRddTest, RddConstructorsAndSpecialOperations) {
  BaseRdd<decltype(*test_plain_rdd_)> copied_base_rdd_1(*test_plain_rdd_, true);
  EXPECT_EQ(copied_base_rdd_1.id(), test_plain_rdd_->id());

  BaseRdd<decltype(*test_plain_rdd_)> copied_base_rdd_2(*test_plain_rdd_, false);
  EXPECT_NE(copied_base_rdd_2.id(), test_plain_rdd_->id());

  BaseRdd<decltype(*test_plain_rdd_)> copied_base_rdd_3(*test_plain_rdd_);
  EXPECT_EQ(copied_base_rdd_3.id(), test_plain_rdd_->id());

  auto copied_plain_rdd(*test_plain_rdd_);
  EXPECT_EQ(copied_plain_rdd.id(), test_plain_rdd_->id());
}

TEST_F(BaseRddTest, SplitBasicOperations) {
  const auto& split = test_plain_rdd_->front();
  EXPECT_FALSE(split.empty());
  EXPECT_TRUE(split);
  EXPECT_EQ(split.size(), test_data_size_ / test_config_.getParallelTaskNum());
  EXPECT_EQ(*split.begin(), split.front());
  EXPECT_EQ(split.begin() + split.size(), split.end());

  // for (size_t i : std::views::iota(0, split.size())) {
  //   EXPECT_EQ(split[i], test_original_data_[i]);
  // }
}

TEST_F(BaseRddTest, SplitConstructorsAndSpecialOperations) {
  const auto& split = test_plain_rdd_->front();
  BaseSplit<decltype(split)> copied_base_split_1(split, true, false);
  EXPECT_EQ(copied_base_split_1.id(), split.id());

  BaseSplit<decltype(split)> copied_base_split_2(split, false, false);
  EXPECT_NE(copied_base_split_2.id(), split.id());

  BaseSplit<decltype(split)> copied_base_split_3(split);
  EXPECT_EQ(copied_base_split_3.id(), split.id());
}

TEST_F(BaseRddTest, CachedSplitCorrectness) {
  // TODO.
}

TEST_F(BaseRddTest, PlainSplitCorrectness) {
  size_t i = 0u;
  for (const auto& split : *test_plain_rdd_) {
    for (const auto& val : split) {
      EXPECT_EQ(val, test_original_data_[i++]);
    }
  }
}
