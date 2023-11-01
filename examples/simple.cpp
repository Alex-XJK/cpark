#include <iostream>
#include <ranges>
#include "rdd.h"

int main() {
  using namespace cpark;

  // Simple use cases.

  auto transformed_iota = std::views::iota(0, 100) | std::views::transform([](auto x) { return x * x; });

  auto plain_rdd = PlainRdd(transformed_iota);
  for (auto x : plain_rdd) {
    std::cout << x << std::endl;
  }
  for (auto x : plain_rdd.get_split(7)) {
    std::cout << x << std::endl;
  }

  auto generator = GeneratorRdd(0, 50, [](auto x) { return std::to_string(x) + " hello!\n"; });
  for (const auto& x : generator.get_split(6)) {
    std::cout << x;
  }

  auto tr = TransformedRdd(generator, [](const auto& x) { return x + "world\n"; });
  for (const auto& x : tr) {
    std::cout << x;
  }

  int res = plain_rdd | Reduce([](int x, int y) { return x + y; });
  std::cout << "reduced value is " << res << std::endl;
}
