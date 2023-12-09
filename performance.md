# Performance Showcase
Located in the examples folder in our source code, we provided a performance showcase program, `speed_check.cpp`, to demonstrate the performance of our cpark library.

## Performance Test

### Program Purpose

Generate values from 1 to N, 
compute its square value, 
sum up from 1 to this value, 
filter all numbers that can be divided by 5, 
add 2 to each of them, 
filter all numbers that can be divided by 3, 
compute reduce on this sequence.

### C++ STL Version
```c
auto cpp_std_view =
    std::views::iota(1, N + 1) |
    std::views::transform([](auto x) { return x * x; }) |
    std::views::transform([](auto x) {
      int res = 0;
      for (int i = 1; i <= x; i++) res += x;
      return res;
    }) |
    std::views::filter([](auto x) { return x % 5 == 0; }) |
    std::views::transform([](auto x) { return x + 2; }) |
    std::views::filter([](auto x) { return x % 3 == 0; });
auto cpp_result = std::reduce(cpp_std_view.begin(), cpp_std_view.end(), 0, [](auto x, auto y) { return x + y; });
```

### cpark Version
```c
cpark::Config default_config;
default_config.setParallelTaskNum(C);
cpark::ExecutionContext default_context{default_config};

auto cpark_result =
    cpark::GeneratorRdd(1, N + 1, [&](auto i) -> auto { return i; }, &default_context) |
    cpark::Transform([](auto x) { return x * x; }) |
    cpark::Transform([](auto x) {
      int res = 0;
      for (int i = 1; i <= x; i++) res += x;
      return res;
    }) |
    cpark::Filter([](auto x) { return x % 5 == 0; }) |
    cpark::Transform([](auto x) { return x + 2; }) |
    cpark::Filter([](auto x) { return x % 3 == 0; }) |
    cpark::Reduce([](auto x, auto y) { return x + y; });
```

### Result
Tested on a 12-core machine and compiled with -O3 flag using a clang-1500.0.40.1 compiler,
when N = 5000000, we got the following timing result:
```
C++ standard way uses 29979 ms
CPARK (1 core) uses 54311 ms    [1.8116x]
CPARK (3 cores) uses 15752 ms   [0.5254x]
CPARK (5 cores) uses 9412 ms    [0.3140x]
CPARK (7 cores) uses 6777 ms    [0.2261x]
CPARK (9 cores) uses 7821 ms    [0.2609x]
CPARK (11 cores) uses 7004 ms   [0.2336x]
Hardware concurrency : 12
CPARK (13 cores) uses 7162 ms   [0.2389x]
CPARK (15 cores) uses 6449 ms   [0.2151x]
CPARK (17 cores) uses 6783 ms   [0.2263x]
CPARK (19 cores) uses 5945 ms   [0.1983x]
CPARK (21 cores) uses 5727 ms   [0.1910x]
CPARK (23 cores) uses 5908 ms   [0.1971x]
```
(The last column is the speedup ratio compared to the C++ standard version)