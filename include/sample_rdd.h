

#ifndef CPARK_SAMPLE_RDD_H
#define CPARK_SAMPLE_RDD_H

#include <iostream>
#include <random>
#include "filter_rdd.h"
#include "generator_rdd.h"


namespace cpark {

template <concepts::Rdd R>
class SampleRdd : public BaseRdd<SampleRdd<R>>{
public:
    using Base = BaseRdd<SampleRdd<R>>;
    friend Base;

    SampleRdd(const R& prev, double probability) : Base{prev, false}, probability_{probability} {
   
    auto sample = [](int i){
            std::random_device rd;
            std::mt19937 gen(rd());
            std::bernoulli_distribution d(0.1);
            return d(gen);
        };

    std::function<bool(int)> sampleFunction = sample;

    FilterRdd<R, std::function<bool(int)>> filter_rdd = FilterRdd(prev, sampleFunction);

    for (const concepts::Split auto& prev_split : filter_rdd) {
        splits_.emplace_back(prev_split, prev_split);
        splits_.back().addDependency(prev_split);
       
        }

    };

    constexpr SampleRdd(const SampleRdd&) = default;
    SampleRdd& operator=(const SampleRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
    using SampleViewType = std::ranges::range_value_t<R>;
    std::vector<ViewSplit<SampleViewType>> splits_{};
    double probability_;

};

}  // namespace cpark

#endif  // CPARK_SAMPLE_RDD_H


