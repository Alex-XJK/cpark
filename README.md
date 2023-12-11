# Welcome to cpark: Supercharge Your Parallel Computing in C++

[![CMake on multiple platforms](https://github.com/Alex-XJK/cpark/actions/workflows/cmake-multi-platform.yml/badge.svg?branch=main)](https://github.com/Alex-XJK/cpark/actions/workflows/cmake-multi-platform.yml)
[![codecov](https://codecov.io/gh/Alex-XJK/cpark/graph/badge.svg?token=L0FVLL29MN)](https://codecov.io/gh/Alex-XJK/cpark)
[![CodeQL](https://github.com/Alex-XJK/cpark/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/Alex-XJK/cpark/actions/workflows/github-code-scanning/codeql)

## About cpark

**cpark** 
is a high-performance, parallel computing library for C++ developed by passionate students at Columbia University. 
Inspired by Apache Spark, our goal is to empower developers with a lightning-fast, easy-to-use framework for fast and general-purpose large data computing in C++.

## Authors
- **Mr. Shichen Xu** ([link](https://www.linkedin.com/in/shichen-xu-9b50a8179/))  
- **Mr. Jiakai Xu** ([link](https://www.alexxu.tech/))  
- **Mr. Xintong Zhan** ([link](https://www.linkedin.com/in/xintong-zhan-060035250/))

## Features

- **Blazing Fast:** Benchmark-proven to be 80% faster than the standard C++ ranges library.
- **Local Multi-threading:** Harness the power of parallel computing on your local machine with multi-threading support.
- **Ease of Use:** Simple and intuitive API, making parallel computing accessible to all developers.
- **Scalability:** Designed to scale effortlessly on a single machine.

## Getting Started

1. **Check Documentation Website:**  
  [www.alexxu.tech/cpark](https://www.alexxu.tech/cpark/)

2. **Get our CPARK:**
   ```bash
   git clone https://github.com/Alex-XJK/cpark.git
   ```

3. **Write your Code:**
   ```cpp
    #include <iostream>
   
    #include "generator_rdd.h"
    #include "transformed_rdd.h"
    #include "filter_rdd.h"
    #include "reduce.h"
    
    int main() {
        cpark::Config default_config;
        default_config.setParallelTaskNum(8);
        cpark::ExecutionContext default_context{default_config};
        
        auto result =
            cpark::GeneratorRdd(1, 1000, [&](auto i) -> auto { return i; }, &default_context) |
            cpark::Transform([](auto x) { return x * x; }) |
            cpark::Filter([](auto x) { return x % 3 == 0; }) |
            cpark::Reduce([](auto x, auto y) { return x + y; });
        
        std::cout << "The computation result is " << result << std::endl;
        
        return 0;
    }
   ```

4. **Config Compiler**  
    Since our project makes the use of the standard library <ranges> in C++20. Your local compiler has to be able to support C++20, with correct compilation flag set to this or newer version, for example, `-std=c++20`.

5. **Include the Headers**  
    Manage your compiler to look for the correct header file location, for example, `-I <path_to_cpark>/include`.

## Community and Support

- **Issue Tracker:** Found a bug or have a feature request? [Create an issue](https://github.com/Alex-XJK/cpark/issues) and let us know.
- **Contact:** For administration issue, you can contact us by e-mail: [cpark@alexxu.tech](mailto:cpark@alexxu.tech).
- **Contribute:** If you are interested in our [project](https://github.com/Alex-XJK/cpark), we would appreciate it if you star our project and submit a pull request for your code.

## Test Status

### Code Coverage
![Coverage](https://codecov.io/gh/Alex-XJK/cpark/graphs/icicle.svg?token=L0FVLL29MN)

## Acknowledgments

We would like to express our gratitude to our project supervisor and designer of C++ 
[Prof. Bjarne Stroustrup](https://www.stroustrup.com/)
and everyone who has contributed to making **cpark** a reality.

