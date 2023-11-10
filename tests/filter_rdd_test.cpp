#include "filter_rdd.h"
#include "cpark.h"
#include "generator_rdd.h"
#include "gtest/gtest.h"

using namespace cpark;

TEST(FilterRdd, FilterRddCorrectness) {
  ExecutionContext default_context{};
  auto even = [](int i) {
    return 0 == i % 2;
  };
  auto generator_rdd = GeneratorRdd(
      0, 1000 + 1, [](auto x) { return x; }, &default_context);
  auto filter_rdd = FilterRdd(generator_rdd, even);
  int sum = 0, size = 0;
  for (const concepts::Split auto& split : filter_rdd) {
    for (const auto& x : split) {
      sum += x;
      size += 1;
    }
  }
  EXPECT_EQ(501, size);
  EXPECT_EQ(250500, sum);
}
