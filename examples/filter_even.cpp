#include <iostream>

#include "filter_rdd.h"
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

    // Creates a generator rdd, who holds 50 strings, each containing a number and a "hello".
    std::cout << "Generator rdd : " << std::endl;
    auto generator_rdd = GeneratorRdd(
    0, 50 + 1, [](auto x) {
        return x;
    }, &default_context);
    printRdd(generator_rdd);

    auto even = [](int i) {
        return 0 == i % 2;
    };

    // Test for FilterRdd(const R& prev, Func func) constructor
    std::cout << "Filter rdd (basic): " << std::endl;
    auto filter_rdd_1 = FilterRdd(generator_rdd, even);
    printRdd(filter_rdd_1);

    // Test for pipeline and Filter(Func func) operators
    std::cout << "Filter rdd (operators): " << std::endl;
    auto filter_rdd_2 = generator_rdd | Filter(even);
    printRdd(filter_rdd_2);

    return 0;
}
