#include <iostream>
#include <ranges>
#include <chrono>

#include "plain_rdd.h"
#include "transformed_rdd.h"
#include "filter_rdd.h"
#include "merge_rdd.h"

inline std::chrono::time_point<std::chrono::high_resolution_clock> getCurrentTime() {
  return std::chrono::high_resolution_clock::now();
}

inline long getTimeDifference(
    std::chrono::time_point<std::chrono::high_resolution_clock> t0,
    std::chrono::time_point<std::chrono::high_resolution_clock> t1) {
  return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
}

int main() {
  int N = 10000;

  /*
   * Using the ranges and views in standard C++,
   * generate values from 1 to N,
   * compute its square value,
   * filter all numbers that can be divided by 5,
   * add 2 to each of them,
   * filter all numbers that can be divided by 3.
   */
  auto std_begin_ts = getCurrentTime();

  auto cpp_std_view =
      std::views::iota(1, N + 1) |
      std::views::transform([](auto x) { return x * x; }) |
      std::views::filter([](auto x) { return x % 5 == 0; }) |
      std::views::transform([](auto x) { return x + 2; }) |
      std::views::filter([](auto x) { return x % 3 == 0; });

  for (auto x: cpp_std_view)
    std::cout << x << " ";
  std::cout << std::endl;

  auto std_end_ts = getCurrentTime();

  /*
   * Using our CPARK method to repeat everything again.
   */

  cpark::Config default_config;
  default_config.setDebugName("My CPARK");
  default_config.setParallelTaskNum();
  cpark::ExecutionContext default_context{default_config};

  auto cpark_begin_ts = getCurrentTime();

  auto base_view =
      std::views::iota(1, N + 1);
  auto cpark_view =
      cpark::PlainRdd(base_view, &default_context) |
      cpark::Transform([](auto x) { return x * x; }) |
      cpark::Filter([](auto x) { return x % 5 == 0; }) |
      cpark::Transform([](auto x) { return x + 2; }) |
      cpark::Filter([](auto x) { return x % 3 == 0; });

  for (const auto& s: cpark_view)
    for (auto x: s)
      std::cout << x << " ";
  std::cout << std::endl;

  auto cpark_end_ts = getCurrentTime();

  /*
   * Compare running time.
   */
  std::cerr << "C++ standard way uses " << getTimeDifference(std_begin_ts, std_end_ts) << " ms\n";
  std::cerr << "CPARK uses " << getTimeDifference(cpark_begin_ts, cpark_end_ts) << " ms\n";

  return 0;
}
