#include <iostream>
#include "gtest/gtest.h"

#include "cpark.h"

#include "filter_rdd.h"
#include "plain_rdd.h"

#include "count.h"

using namespace cpark;

TEST(CountTest, CountTest1) {
  ExecutionContext default_context{};

  const int N = 20000;
  auto iota_view = std::views::iota(1, N + 1);
  unsigned long long v_cnt = std::ranges::size(iota_view);

  concepts::Rdd auto plain_rdd = PlainRdd(iota_view, &default_context);
  unsigned long long c_cnt = plain_rdd | Count();

  EXPECT_EQ(c_cnt, v_cnt);
  EXPECT_EQ(N, c_cnt);
}
