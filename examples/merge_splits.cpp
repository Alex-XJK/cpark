#include <iostream>
#include <ranges>

#include "plain_rdd.h"
#include "filter_rdd.h"
#include "union_rdd.h"
#include "merge_rdd.h"

template <cpark::concepts::Rdd R>
void printRdd(R rdd) {
  for (const cpark::concepts::Split auto& split : rdd) {
    std::cout << "Split #" << split.id() << " :\t";
    std::cout << "(len: " << split.size() << ")\t";
    for (const auto& x : split)
      std::cout << x << "\t";
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

int main() {
  using namespace cpark;

  // Creates execution contexts.
  Config default_config;
  default_config.setParallelTaskNum();
  ExecutionContext default_context{default_config};

  // Creates a plain Rdd from a view, which contains the same data as the view.
  auto iota_view_1 =
      std::views::iota(1, 50);
  concepts::Rdd auto plain_rdd_1 = PlainRdd(iota_view_1, &default_context);

  auto iota_view_2 =
      std::views::iota(50, 100);
  concepts::Rdd auto plain_rdd_2 = PlainRdd(iota_view_2, &default_context);

  // Print out original splits inside the rdd.
  std::cout << "The plain Rdd 1 has " << plain_rdd_1.size() << " splits." << std::endl;
  printRdd(plain_rdd_1);
  std::cout << "The plain Rdd 2 has " << plain_rdd_2.size() << " splits." << std::endl;
  printRdd(plain_rdd_2);

  // Filter rdd
  auto even = [](int i) { return 0 == i % 2; };
  concepts::Rdd auto filter_rdd_1 = plain_rdd_1 | Filter(even);
  concepts::Rdd auto filter_rdd_2 = plain_rdd_2 | Filter(even);

  // Union rdd
  concepts::Rdd auto union_rdd = UnionRdd(filter_rdd_1, filter_rdd_2);
  std::cout << "The union Rdd has " << union_rdd.size() << " splits." << std::endl;
  printRdd(union_rdd);

  // Merge splits
  concepts::Rdd auto merge_rdd = union_rdd | Merge();
  std::cout << "The merge Rdd has " << merge_rdd.size() << " splits." << std::endl;
  printRdd(merge_rdd);

  return 0;
}
