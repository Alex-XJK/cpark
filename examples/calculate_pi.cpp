#include <iostream>
#include <random>

#include "cpark.h"
#include "generator_rdd.h"
#include "reduce.h"

int main() {
  // Parallelly calculate the value of pi by series with n terms.
  int n = 100000000;
  cpark::ExecutionContext context(cpark::Config().setParallelTaskNum(8));

  double pi =
      cpark::GeneratorRdd(
          0, n, [&](int i) -> double { return 4.0 / (2 * i + 1) * ((i & 1) ? -1 : 1); }, &context) |
      cpark::Reduce([](auto x, auto y) { return x + y; });

  std::cout << "The value of pi is roughly " << pi << std::endl;
}
