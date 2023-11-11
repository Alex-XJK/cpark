#include <iostream>
#include <ranges>

#include "plain_rdd.h"
#include "union_rdd.h"

template <cpark::concepts::Rdd R>
void printRdd(R rdd) {
  for (const cpark::concepts::Split auto& split : rdd) {
    std::cout << "Split #" << split.id() << " : ";
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

  // Creates a plain Rdd from a view, which contains the same data as the view.
  auto transformed_iota_view_1 =
      std::views::iota(1, 10 + 1) | std::views::transform([](auto x) { return x * x; });
  concepts::Rdd auto plain_rdd_1 = PlainRdd(transformed_iota_view_1, &default_context);

  auto transformed_iota_view_2 =
      std::views::iota(20, 30 + 1) | std::views::transform([](auto x) { return x * x; });
  concepts::Rdd auto plain_rdd_2 = PlainRdd(transformed_iota_view_2, &default_context);

  // Print out original splits inside the rdd.
  std::cout << "The plain Rdd 1 has " << plain_rdd_1.size() << " splits." << std::endl;
  printRdd(plain_rdd_1);
  std::cout << "The plain Rdd 2 has " << plain_rdd_2.size() << " splits." << std::endl;
  printRdd(plain_rdd_2);

  // Calculate the sum of the plain rdd by reduce.
  concepts::Rdd auto union_rdd = UnionRdd(plain_rdd_1, plain_rdd_2);
  std::cout << "The union Rdd has " << union_rdd.size() << " splits." << std::endl;
  printRdd(union_rdd);

  return 0;
}
