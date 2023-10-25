#include <vector>
#include <ranges>
#include <memory>
#include <random>
#include <iostream>
#include "rdd.h"

int main() {
    std::random_device random_engine;
    std::uniform_real_distribution<double> distribution(-1, 1);

    // Parallelly calculate the value of pi by monte carlo algorithm with n samples.
    int n = 1'000'000;
    double pi =
        (cpark::GeneratorRdd(0, n, [&](auto) -> double {
            double x = distribution(random_engine), y = distribution(random_engine);
            return x * x + y * y <= 1 ? 1 : 0;
        }) | cpark::Reduce([](auto x, auto y){return x + y;})) / n * 4;

    std::cout << "The value of pi is roughly " << pi << std::endl;
}
