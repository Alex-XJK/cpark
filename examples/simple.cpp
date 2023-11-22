#include <iostream>
#include <ranges>
#include <variant>

#include "generator_rdd.h"
#include "plain_rdd.h"
#include "reduce.h"
#include "transformed_rdd.h"
#include "zipped_rdd.h"
int main() {
  using namespace cpark;

  // Simple use cases of cpark components.

  // Creates configuration objects.
  Config default_config;
  Config customized_config =
      Config().setDebugName("My CPARK!").setParallelTaskNum(16).setLogger(&std::cout);

  // Set and get configuration fields.
  default_config.setDebugName("My default CPARK!");
  std::cout << "The debug name of customized config is " << customized_config.getDebugName()
            << std::endl;

  // Creates execution contexts.
  ExecutionContext default_context{};
  ExecutionContext configured_context{default_config};

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

  // Creates a generator rdd, who holds 50 strings, each containing a number and a "hello".
  std::cout << "The first split of the generator rdd contains the following elements: ";
  auto generator_rdd = GeneratorRdd(
      0, 50, [](auto x) { return std::to_string(x) + " hello"; }, &default_context);
  for (const std::string& x : generator_rdd.front()) {
    std::cout << x << ", ";
  }
  std::cout << std::endl;

  // Creates a transformed rdd which adds a " world" string to the elements in the generator rdd.
  auto transformed_rdd = TransformedRdd(generator_rdd, [](const auto& x) { return x + " world"; });
  std::cout << "The elements in the fourth split of transformed rdd are: ";
  for (const auto& x : transformed_rdd[3]) {
    std::cout << x << ", ";
  }
  std::cout << std::endl;


  auto zippped_rdd = ZippedRdd(plain_rdd, plain_rdd);
  std::cout << "The elements in the fourth split of transformed rdd are: ";
  for (const auto& x : zippped_rdd[3]) {
    std::cout << x << ", ";
  }
  std::cout << std::endl;

  // Creates a transformed rdd using pipeline operator.
  auto transformed_rdd_2 = generator_rdd | Transform([](const auto& x) { return x + " my world"; });
  std::cout << "The elements in the third split of another transformed rdd are: ";
  for (const auto& x : transformed_rdd_2[2]) {
    std::cout << x << ", ";
  }
  std::cout << std::endl;

  // Calculate the sum of the plain rdd by reduce.
  int res = plain_rdd | Reduce([](int x, int y) { return x + y; });
  std::cout << "The sum of the plain rdd is " << res << std::endl;
}
