#include <iostream>
#include <ranges>
#include <chrono>

#include "generator_rdd.h"
#include "transformed_rdd.h"
#include "filter_rdd.h"
#include "merge_rdd.h"
#include "reduce.h"

inline std::chrono::time_point<std::chrono::high_resolution_clock> getCurrentTime() {
  return std::chrono::high_resolution_clock::now();
}

inline long getTimeDifference(
    std::chrono::time_point<std::chrono::high_resolution_clock> t0,
    std::chrono::time_point<std::chrono::high_resolution_clock> t1) {
  return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
}

int main() {
  int N = 30000;
  auto reduce_func = [](auto x, auto y) { return x + y; };

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
  auto cpp_result = std::reduce(cpp_std_view.begin(), cpp_std_view.end(), 0, reduce_func);

  auto std_end_ts = getCurrentTime();

  std::cout << cpp_result << std::endl;

  /*
   * Using our CPARK method to repeat everything again.
   */

  cpark::Config default_config;
  default_config.setDebugName("My CPARK");
  default_config.setParallelTaskNum();
  cpark::ExecutionContext default_context{default_config};

  auto cpark_begin_ts = getCurrentTime();

  auto cpark_result =
      cpark::GeneratorRdd(1, N + 1, [&](int i) -> int { return i; }, &default_context) |
      cpark::Transform([](auto x) { return x * x; }) |
      cpark::Filter([](auto x) { return x % 5 == 0; }) |
      cpark::Transform([](auto x) { return x + 2; }) |
      cpark::Filter([](auto x) { return x % 3 == 0; }) |
      cpark::Reduce(reduce_func);

  auto cpark_end_ts = getCurrentTime();

  std::cout << cpark_result << std::endl;

  /*
   * Compare running time.
   */
  std::cerr << "C++ standard way uses " << getTimeDifference(std_begin_ts, std_end_ts) << " ms\n";
  std::cerr << "CPARK uses " << getTimeDifference(cpark_begin_ts, cpark_end_ts) << " ms\n";

  return 0;
}
