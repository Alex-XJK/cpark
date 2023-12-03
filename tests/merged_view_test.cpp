#include <string>

#include "gtest/gtest.h"
#include "merged_view.h"

using std::operator"" s;
using cpark::MergedSameView;
using cpark::MergedTwoDiffView;
using cpark::MergedDiffView;

TEST(MergedViewTest, MergedSameViewInt) {
  const int view_num = 10;
  const int num_each_view = 100;
  auto test_original_views = std::views::iota(0, view_num) | std::views::transform([](int i) {
                               return std::views::iota(i * num_each_view, (i + 1) * num_each_view);
                             });
  auto merged_view = MergedSameView(test_original_views);
  EXPECT_EQ(merged_view.front(), 0);
  EXPECT_EQ(*merged_view.begin(), 0);
  int i = 0;
  for (int x : merged_view) {
    EXPECT_EQ(x, i);
    i++;
  }
  EXPECT_EQ(i, view_num * num_each_view);
}

TEST(MergedViewTest, MergedSameViewString) {
  const int view_num = 10;
  const int num_each_view = 100;
  auto test_original_views =
      std::views::iota(0, view_num) | std::views::transform([](int i) {
        return std::views::iota(i * num_each_view, (i + 1) * num_each_view) |
               std::views::transform([](int i) { return std::to_string(i); });
      });
  auto merged_view = MergedSameView(test_original_views);
  EXPECT_EQ(merged_view.front(), "0"s);
  EXPECT_EQ(*merged_view.begin(), "0"s);
  int i = 0;
  for (const std::string& x : merged_view) {
    EXPECT_EQ(x, std::to_string(i));
    i++;
  }
  EXPECT_EQ(i, view_num * num_each_view);
}

TEST(MergedViewTest, MergedTwoDiffIntSameType) {
  auto test_original_view1 = std::views::iota(0, 10);
  auto test_original_view2 = std::views::iota(10, 20);
  auto merged_view = MergedTwoDiffView(test_original_view1, test_original_view2);
  int i = 0;
  for (int x : merged_view) {
    EXPECT_EQ(x, i);
    i++;
  }
  EXPECT_EQ(i, 20);
}

TEST(MergedViewTest, MergedTwoDiffIntDifferentType) {
  auto test_original_view1 = std::views::iota(0, 10);
  std::vector<int> test_original_data2{10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
  auto test_original_view2 = std::ranges::subrange(test_original_data2);
  auto merged_view = MergedTwoDiffView(test_original_view1, test_original_view2);
  int i = 0;
  for (int x : merged_view) {
    EXPECT_EQ(x, i);
    i++;
  }
  EXPECT_EQ(i, 20);
}

TEST(MergedViewTest, MergedTwoDiffStringDifferentType) {
  auto test_original_view1 =
      std::views::iota(0, 10) | std::views::transform([](int i) { return std::to_string(i); });
  std::vector<std::string> test_original_data2{"10", "11", "12", "13", "14",
                                               "15", "16", "17", "18", "19"};
  auto test_original_view2 = std::ranges::subrange(test_original_data2);
  auto merged_view = MergedTwoDiffView(test_original_view1, test_original_view2);
  int i = 0;
  for (const std::string& x : merged_view) {
    EXPECT_EQ(x, std::to_string(i));
    i++;
  }
  EXPECT_EQ(i, 20);
}

TEST(MergedViewTest, MergedVariousDiffIntDifferentType) {
  auto test_original_view1 = std::views::iota(0, 10);
  std::vector<int> test_original_data2{10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
  auto test_original_view2 = std::ranges::subrange(test_original_data2);
  auto test_original_view3 = std::views::iota(20, 30);
  auto merged_view = MergedDiffView(test_original_view1, test_original_view2, test_original_view3);
  int i = 0;
  for (int x : merged_view) {
    EXPECT_EQ(x, i);
    i++;
  }
  EXPECT_EQ(i, 30);
}
