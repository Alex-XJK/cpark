#include "sample_rdd.h"
#include "cpark.h"
#include "generator_rdd.h"
#include "gtest/gtest.h"

using cpark::GeneratorRdd;
using cpark::SampleRdd;

TEST(SampleRdd, SamplewithFraction1) {
  cpark::ExecutionContext default_context{};
  auto generator_rdd = GeneratorRdd(
      0, 1000 + 1, [](auto x) { return x; }, &default_context);
  auto sample_rdd = SampleRdd(generator_rdd, 1);
  int size = 0;
  for (const cpark::concepts::Split auto& split : sample_rdd) {
    for (const auto& x : split) {
      size += 1;
    }
  }
  EXPECT_EQ(1001, size);
}

TEST(SampleRdd, SamplewithFraction0) {
  cpark::ExecutionContext default_context{};
  auto generator_rdd = GeneratorRdd(
      0, 1000 + 1, [](auto x) { return x; }, &default_context);
  auto sample_rdd = SampleRdd(generator_rdd, 0);
  int size = 0;
  for (const cpark::concepts::Split auto& split : sample_rdd) {
       EXPECT_TRUE(split.begin() == split.end());
      size += 1;
    }
  }
  EXPECT_EQ(0, size);
}
