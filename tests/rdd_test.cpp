#include <iostream>
#include <thread>
#include "gtest/gtest.h"

#include "cpark.h"

#include "filter_rdd.h"
#include "generator_rdd.h"
#include "plain_rdd.h"

#include "count.h"

using namespace cpark;

TEST(config, thread_size) {
  // default
  Config default_config;
  default_config.setDebugName("CPARK gtest");
  default_config.setParallelTaskNum();
  ExecutionContext default_context{default_config};
  auto iota_view = std::views::iota(1, 100);
  concepts::Rdd auto plain_rdd_1 = PlainRdd(iota_view, &default_context);
  unsigned int N = std::thread::hardware_concurrency();
  EXPECT_EQ(N, plain_rdd_1.size());
  // custom
  default_config.setParallelTaskNum(1000);
  ExecutionContext custom_context{default_config};
  concepts::Rdd auto plain_rdd_2 = PlainRdd(iota_view, &custom_context);
  EXPECT_EQ(1000, plain_rdd_2.size());
}

TEST(rdd_suite, plain_rdd_dummy) {
  ExecutionContext default_context{};
  auto transformed_iota_view =
      std::views::iota(1, 100 + 1) | std::views::transform([](auto x) { return x * x; });
  concepts::Rdd auto plain_rdd = PlainRdd(transformed_iota_view, &default_context);

  EXPECT_EQ(1, 1);
}

TEST(action_suite, count_test1) {
  ExecutionContext default_context{};

  const int N = 20000;
  auto iota_view = std::views::iota(1, N + 1);
  unsigned long long v_cnt = std::ranges::size(iota_view);

  concepts::Rdd auto plain_rdd = PlainRdd(iota_view, &default_context);
  unsigned long long c_cnt = plain_rdd | Count();

  EXPECT_EQ(c_cnt, v_cnt);
  EXPECT_EQ(N, c_cnt);

}
