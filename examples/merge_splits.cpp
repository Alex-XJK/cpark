#include <iostream>
#include <ranges>

#include "plain_rdd.h"
#include "merge_rdd.h"

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
  auto transformed_iota_view =
      std::views::iota(1, 100 + 1) | std::views::transform([](auto x) { return x * x; });
  concepts::Rdd auto plain_rdd = PlainRdd(transformed_iota_view, &default_context);

  // Print out original splits inside the rdd.
  std::cout << "The plain Rdd has " << plain_rdd.size() << " splits." << std::endl;
  printRdd(plain_rdd);

  // Calculate the sum of the plain rdd by reduce.
  concepts::Rdd auto merged_rdd = plain_rdd | Merge();
  std::cout << "The merged Rdd has " << merged_rdd.size() << " splits." << std::endl;
  printRdd(merged_rdd);

  return 0;
}
