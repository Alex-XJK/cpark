# Real-world Use Cases (Shell Scripting)
## Build a simple program
```
Home >> ll
total 0

Home >> git clone https://github.com/Alex-XJK/cpark.git
Cloning into 'cpark'...
remote: Enumerating objects: 3087, done.
remote: Counting objects: 100% (911/911), done.
remote: Compressing objects: 100% (320/320), done.
remote: Total 3087 (delta 608), reused 806 (delta 590), pack-reused 2176
Receiving objects: 100% (3087/3087), 2.98 MiB | 6.69 MiB/s, done.
Resolving deltas: 100% (2246/2246), done.

Home >> vim cpark.cpp

Home >> cat cpark.cpp 
#include <iostream>

#include "generator_rdd.h"
#include "transformed_rdd.h"
#include "filter_rdd.h"
#include "reduce.h"

int main() {
    cpark::Config default_config;
    default_config.setParallelTaskNum(4);
    cpark::ExecutionContext default_context{default_config};

    auto cpark_result =
        cpark::GeneratorRdd(1, 100, [&](auto i) -> auto { return i; }, &default_context) |
        cpark::Transform([](auto x) { return x * x; }) |
        cpark::Transform([](auto x) {
          int res = 0;
          for (int i = 1; i <= x; i++) res += x;
          return res;
        }) |
        cpark::Filter([](auto x) { return x % 5 == 0; }) |
        cpark::Reduce([](auto x, auto y) { return x + y; });

    std::cout << cpark_result << std::endl;

    return 0;
}

Home >> ll
total 8
drwxr-xr-x  14 alex  staff   448B Dec  6 10:27 cpark
-rw-r--r--   1 alex  staff   773B Dec  6 10:43 cpark.cpp

Home >> ls ./cpark/include/
base_rdd.h         cpark.h            group_by_key_rdd.h partition_by_rdd.h sample_rdd.h       utils.h
collect.h          filter_rdd.h       merge_rdd.h        plain_rdd.h        transformed_rdd.h  zipped_rdd.h
count.h            generator_rdd.h    merged_view.h      reduce.h           union_rdd.h

Home >> g++ --version
Apple clang version 15.0.0 (clang-1500.0.40.1)
Target: arm64-apple-darwin23.1.0
Thread model: posix
InstalledDir: /Library/Developer/CommandLineTools/usr/bin

Home >> g++ cpark.cpp -std=c++20 -Wall -Wpedantic -O3 -I./cpark/include -o cpark_example 

Home >> ll
total 232
drwxr-xr-x  14 alex  staff   448B Dec  6 10:27 cpark
-rw-r--r--   1 alex  staff   773B Dec  6 10:43 cpark.cpp
-rwxr-xr-x   1 alex  staff   108K Dec  6 10:43 cpark_example

Home >> ./cpark_example 
351666250

Home >> ^D

```