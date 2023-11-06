#include <iostream>
#include <thread>
#include "gtest/gtest.h"

#include "cpark.h"
#include "plain_rdd.h"
#include "generator_rdd.h"
#include "filter_rdd.h"

using namespace cpark;

TEST(config, thread_size) {
    // default
    Config default_config;
    default_config.setDebugName("CPARK gtest");
    default_config.setParallelTaskNum();
    ExecutionContext default_context{default_config};
    auto iota_view = std::views::iota(1, 100);
    concepts::Rdd auto plain_rdd_1 = PlainRdd(iota_view, &default_context);
    unsigned int N = std::thread::hardware_concurrency();
    EXPECT_EQ(N, plain_rdd_1.size());
    // custom
    default_config.setParallelTaskNum(1000);
    ExecutionContext custom_context{default_config};
    concepts::Rdd auto plain_rdd_2 = PlainRdd(iota_view, &custom_context);
    EXPECT_EQ(1000, plain_rdd_2.size());
}

TEST(rdd_suite, plain_rdd_dummy) {
    ExecutionContext default_context{};
    auto transformed_iota_view =
        std::views::iota(1, 100 + 1) | std::views::transform([](auto x) { return x * x; });
    concepts::Rdd auto plain_rdd = PlainRdd(transformed_iota_view, &default_context);

    EXPECT_EQ(1, 1);
}

TEST(rdd_suite, filter_rdd_correctness) {
    ExecutionContext default_context{};
    auto even = [](int i) { return 0 == i % 2; };
    auto generator_rdd = GeneratorRdd(0, 1000 + 1, [](auto x) { return x; }, &default_context);
    auto filter_rdd = FilterRdd(generator_rdd, even);
    int sum = 0, size = 0;
    for (const concepts::Split auto& split : filter_rdd) {
        for (const auto &x: split) {
            sum += x;
            size += 1;
        }
    }
    EXPECT_EQ(501, size);
    EXPECT_EQ(250500, sum);
}
