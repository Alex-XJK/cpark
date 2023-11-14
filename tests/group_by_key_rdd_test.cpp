#include <random>

#include "cpark.h"
#include "generator_rdd.h"
#include "group_by_key_rdd.h"
#include "gtest/gtest.h"
#include "partition_by_rdd.h"
#include "plain_rdd.h"

using cpark::ExecutionContext;
using cpark::GeneratorRdd;
using cpark::GroupByKeyRdd;
using cpark::PartitionByRdd;
using cpark::PlainRdd;

TEST(GroupByKeyRdd, GroupByKeyCorrectness) {
  ExecutionContext default_context{};
  std::vector<std::pair<int, std::string>> test_data;
  for (int i = 0; i < 1000; ++i) {
    test_data.emplace_back(i, std::to_string(i));
    test_data.emplace_back(i, std::to_string(i));
    test_data.emplace_back(i, std::to_string(i));
  }
  std::shuffle(std::begin(test_data), std::end(test_data), std::mt19937(std::random_device()()));
  auto plain_rdd = PlainRdd(std::ranges::subrange(test_data), &default_context);
  auto partition_by_rdd = PartitionByRdd(plain_rdd);
  auto group_by_key_rdd = GroupByKeyRdd(partition_by_rdd);
  int size = 0;
  for (const cpark::concepts::Split auto& split : group_by_key_rdd) {
    for (const auto& [key, values] : split) {
      EXPECT_EQ(values.size(), 3);
      for (const auto& value : values) {
        EXPECT_EQ(value, std::to_string(key));
      }
      ++size;
    }
  }
  EXPECT_EQ(size, 1000);
}
