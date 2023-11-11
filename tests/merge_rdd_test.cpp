#include "cpark.h"
#include "plain_rdd.h"
#include "merge_rdd.h"
#include "gtest/gtest.h"

using namespace cpark;

TEST(MergeRdd, MergeRddCorrectness) {
  ExecutionContext default_context{};

  auto iota_view_1 = std::views::iota(1, 100);
  concepts::Rdd auto plain_rdd_1 = PlainRdd(iota_view_1, &default_context);

  size_t ori_element_count = 0;

  for (const concepts::Split auto& split : plain_rdd_1)
    for (const auto& x : split)
      ori_element_count += 1;

  concepts::Rdd auto union_rdd = plain_rdd_1 | Merge();

  size_t new_length = union_rdd.size();
  size_t new_element_count = 0;

  for (const concepts::Split auto& split : union_rdd)
    for (const auto& x : split)
      new_element_count += 1;

  EXPECT_EQ(new_length, 1);
  EXPECT_EQ(ori_element_count, new_element_count);
}
