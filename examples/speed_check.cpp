#include <iostream>
#include <ranges>
#include <chrono>
#include <thread>

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
  int N = 500000;

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
      std::views::transform([](auto x) {
        int res = 1;
        for (int i = 0; i < 1000; ++i) res *= x;
        return res;
      }) |
      std::views::filter([](auto x) { return x % 5 == 0; }) |
      std::views::transform([](auto x) { return x + 2; }) |
      std::views::filter([](auto x) { return x % 3 == 0; });
  auto cpp_result = std::reduce(cpp_std_view.begin(), cpp_std_view.end(), 0, [](auto x, auto y) { return x + y; });

  auto std_end_ts = getCurrentTime();

  const long cpp_time = getTimeDifference(std_begin_ts, std_end_ts);

  std::cout << cpp_result << std::endl;
  std::cerr << "C++ standard way uses " << cpp_time << " ms\n";

  /*
   * Using our CPARK method to repeat everything again.
   */

  const unsigned int hardware_concurrency = std::thread::hardware_concurrency();

  for (int cores = 1; cores < 2 * hardware_concurrency; cores += 2) {

    cpark::Config default_config;
    default_config.setParallelTaskNum(cores);
    cpark::ExecutionContext default_context{default_config};

    auto cpark_begin_ts = getCurrentTime();

    auto cpark_result =
        cpark::GeneratorRdd(1, N + 1, [&](auto i) -> auto { return i; }, &default_context) |
        cpark::Transform([](auto x) {
          int res = 1;
          for (int i = 0; i < 1000; ++i) res *= x;
          return res;
        }) |
        cpark::Filter([](auto x) { return x % 5 == 0; }) |
        cpark::Transform([](auto x) { return x + 2; }) |
        cpark::Filter([](auto x) { return x % 3 == 0; }) |
        cpark::Reduce([](auto x, auto y) { return x + y; });

    auto cpark_end_ts = getCurrentTime();

    long temp_time = getTimeDifference(cpark_begin_ts, cpark_end_ts);

    std::cout << cpark_result << std::endl;
    std::cerr << "CPARK (" << cores <<" cores) uses " << temp_time << " ms [" << (double)temp_time / cpp_time << "x]\n";

    if (cores == hardware_concurrency || cores == hardware_concurrency - 1)
      std::cerr << "Hardware concurrency : " << hardware_concurrency << "\n";
  }

  return 0;
}
