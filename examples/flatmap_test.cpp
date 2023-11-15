#include <iostream>
#include <vector>

#include "flatmap_rdd.h"
#include "generator_rdd.h"

template <cpark::concepts::Rdd R>
void printRdd(R rdd) {
  for (const cpark::concepts::Split auto& split : rdd) {
    for (const auto& x : split)
      std::cout << x << "\t";
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

int main() {
  using namespace cpark;

  // Creates execution contexts.
  ExecutionContext default_context{};

  // Creates a generator rdd, which holds numbers from 0 to 50.
  std::cout << "Generator rdd: " << std::endl;
  auto generator_rdd = GeneratorRdd(
      0, 50 + 1, [](auto x) { return x; }, &default_context);
  printRdd(generator_rdd);

  // Test for FlatMapRdd(const R& prev, Func func) constructor
  std::cout << "FlatMap rdd (basic): " << std::endl;
  auto flatmap_rdd_1 = FlatMapRdd(generator_rdd, [](int i) {return std::vector<int>(i, i);});
  printRdd(flatmap_rdd_1);

  // Test for pipeline and FlatMap(Func func) operators
  std::cout << "FlatMap rdd (operators): " << std::endl;
  auto flatmap_rdd_2 = generator_rdd | FlatMap([](int i) {return std::vector<int>(i, i);});
  printRdd(flatmap_rdd_2);

  return 0;
}
