#include <iostream>

#include "collect.h"
#include "generator_rdd.h"
#include "plain_rdd.h"

int main() {
  using namespace cpark;

  // Simple use cases of cpark components.

  // Creates configuration objects.
  Config default_config;

  // Set and get configuration fields.
  default_config.setDebugName("My default CPARK!");

  // Creates execution contexts.
  ExecutionContext default_context{};


  // Creates a plain Rdd from a view, which contains the same data as the view.
  auto transformed_iota_view =
      std::views::iota(1, 100 + 1) | std::views::transform([](auto x) { return x * x; });
  concepts::Rdd auto plain_rdd = PlainRdd(transformed_iota_view, &default_context);

  // Get the number of splits inside the rdd.
  std::cout << "The plain Rdd has " << plain_rdd.size() << " splits." << std::endl;

  // Get the splits inside the rdd.
  concepts::Split auto first_plain_split = plain_rdd.front();
  concepts::Split auto second_plain_split = plain_rdd[2];
  concepts::Split auto last_plain_split = plain_rdd.back();
  auto iterator_to_first_plain_split = plain_rdd.begin();
  auto iterator_past_last_plain_split = plain_rdd.end();

  // Get elements inside the split.
  auto iterator_to_first_element = first_plain_split.begin();
  int first_element = first_plain_split.front();
  std::cout << "The first split of plain rdd contains the following elements: ";
  for (int x : first_plain_split) {
    std::cout << x << ", ";
  }
  std::cout << std::endl;
  std::cout << "The last split of plain rdd contains the following elements: ";
  for (int x : plain_rdd.back()) {
    std::cout << x << ", ";
  }
  std::cout << std::endl;

 // Test for Collect Rdd constructor
  std::cout << "Collect rdd (basic): " << std::endl;
  auto collect_rdd_1 = plain_rdd | Collect( );
  std::cout <<"element in collection: " << std::endl;
  for(auto c : collect_rdd_1) {
    std::cout << c << std::endl;
  }

}
