#include "cpark.h"
#include "generator_rdd.h"
#include "zipped_rdd.h"
#include "gtest/gtest.h"
#include <typeinfo>

using cpark::GeneratorRdd;
using cpark::ZippedRdd;

TEST(ZippedRdd, ZippedRddCorrectness) {
  cpark::ExecutionContext default_context{};
  auto generator_rdd_1 = GeneratorRdd(
      0, 1000 + 1, [](auto x) { return x; }, &default_context);
  auto generator_rdd_2 = GeneratorRdd(
      0, 1000 + 1, [](auto x) { return x; }, &default_context);
  auto zipped_rdd = ZippedRdd(generator_rdd_1,generator_rdd_2);
  int size = 0;
  for (const cpark::concepts::Split auto& split : zipped_rdd) {
    for (const auto& x : split) {
      size += 1;
    }
  }
  EXPECT_EQ(1001, size);
  EXPECT_EQ(typeid(decltype(zipped_rdd[0].front())),typeid(std::tuple<int,int>));
}

