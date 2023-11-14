#include "partition_by_rdd.h"
#include "cpark.h"
#include "generator_rdd.h"
#include "gtest/gtest.h"

using cpark::ExecutionContext;
using cpark::GeneratorRdd;
using cpark::PartitionByRdd;

TEST(PartitionByRdd, PartitionByRddCorrectness) {
  ExecutionContext default_context{};
  auto even = [](int i) {
    return 0 == i % 2;
  };
  auto generator_rdd = GeneratorRdd(
      0, 1000,
      [](auto x) {
        return std::pair{x, std::to_string(x)};
      },
      &default_context);
  auto partition_by_rdd = PartitionByRdd(generator_rdd);
  int size = 0;
  for (int i = 0; i < partition_by_rdd.size(); ++i) {
    const cpark::concepts::Split auto& split = partition_by_rdd[i];
    for (const auto& [key, value] : split) {
      EXPECT_EQ(std::to_string(key), value);
      EXPECT_EQ(std::hash<int>{}(key) % partition_by_rdd.size(), i);
      ++size;
    }
  }
  EXPECT_EQ(size, 1000);
}
