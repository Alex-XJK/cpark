#include <numeric>

#include "cpark.h"
#include "plain_rdd.h"
#include "union_rdd.h"
#include "gtest/gtest.h"

using namespace cpark;

TEST(UnionRdd, UnionRddCorrectness) {
  ExecutionContext default_context{};

  auto iota_view_1 = std::views::iota(1, 50);
  concepts::Rdd auto plain_rdd_1 = PlainRdd(iota_view_1, &default_context);

  auto iota_view_2 = std::views::iota(50, 100);
  concepts::Rdd auto plain_rdd_2 = PlainRdd(iota_view_2, &default_context);

  size_t ori1_length = plain_rdd_1.size();
  size_t ori2_length = plain_rdd_2.size();
  size_t ori_length = ori1_length + ori2_length;
  size_t ori_element_count = 0;

  for (const concepts::Split auto& split : plain_rdd_1)
    for (const auto& x : split)
      ori_element_count += 1;

  for (const concepts::Split auto& split : plain_rdd_2)
    for (const auto& x : split)
      ori_element_count += 1;


  concepts::Rdd auto union_rdd = UnionRdd(plain_rdd_1, plain_rdd_2);

  size_t new_length = union_rdd.size();
  size_t new_element_count = 0;

  for (const concepts::Split auto& split : union_rdd)
    for (const auto& x : split)
      new_element_count += 1;

  EXPECT_EQ(ori_length, new_length);
  EXPECT_EQ(ori_element_count, new_element_count);
}

TEST(UnionRdd, UnionRddDifferentTypes) {
  ExecutionContext default_context{};

  auto iota_view_1 = std::views::iota(1, 50);
  concepts::Rdd auto plain_rdd_1 = PlainRdd(iota_view_1, &default_context);

  std::vector<int> test_data_2(50);
  std::iota(std::begin(test_data_2), std::end(test_data_2), 50);
  // iota_view_2's type is not the same with iota_view_1.
  auto iota_view_2 = std::ranges::subrange(test_data_2);

  concepts::Rdd auto plain_rdd_2 = PlainRdd(iota_view_2, &default_context);

  size_t ori1_length = plain_rdd_1.size();
  size_t ori2_length = plain_rdd_2.size();
  size_t ori_length = ori1_length + ori2_length;
  size_t ori_element_count = 0;

  for (const concepts::Split auto& split : plain_rdd_1)
    for (const auto& x : split)
      ori_element_count += 1;

  for (const concepts::Split auto& split : plain_rdd_2)
    for (const auto& x : split)
      ori_element_count += 1;


  concepts::Rdd auto union_rdd = UnionRdd(plain_rdd_1, plain_rdd_2);

  size_t new_length = union_rdd.size();
  size_t new_element_count = 0;

  for (const concepts::Split auto& split : union_rdd)
    for (const auto& x : split)
      new_element_count += 1;

  EXPECT_EQ(ori_length, new_length);
  EXPECT_EQ(ori_element_count, new_element_count);
}
