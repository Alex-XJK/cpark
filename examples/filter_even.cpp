#include <iostream>
#include <ranges>
#include <variant>

#include "generator_rdd.h"
#include "filter_rdd.h"

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

    // Creates a generator rdd, who holds 50 strings, each containing a number and a "hello".
    std::cout << "The first split of the generator rdd contains the following elements: ";
    auto generator_rdd = GeneratorRdd(0, 50, [](auto x) { return x; }, &default_context);
    for (const auto& x : generator_rdd.front()) {
        std::cout << x << ", ";
    }
    std::cout << std::endl;

    bool even = [](int i) { return 0 == i % 2; };

    // Test for FilterRdd(const R& prev, Func func) constructor
    auto filter_rdd_1 = FilterRdd(generator_rdd, even);
    for (const auto& x : filter_rdd_1.front()) {
        std::cout << x << ", ";
    }
    std::cout << std::endl;

//    // Test for pipeline and Filter(Func func) operators
//    auto filter_rdd_2 = generator_rdd | Filter(even);
//    for (const auto& x : filter_rdd_2.front()) {
//        std::cout << x << ", ";
//    }
//    std::cout << std::endl;
}
