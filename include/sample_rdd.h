#ifndef CPARK_SAMPLE_RDD_H
#define CPARK_SAMPLE_RDD_H

#include <random>
#include "filter_rdd.h"

namespace cpark {

template <concepts::Rdd R>
class SampleRdd : public BaseRdd<SampleRdd<R>>{
public:
    using Base = BaseRdd<SampleRdd<R>>;
    friend Base;

    SampleRdd(const R& prev, double probability) : Base{prev, false}, probability_{probability} {
   
    sample = [this](int i){
            std::random_device rd;
            std::mt19937 gen(rd());
            std::bernoulli_distribution d(probability_);
            return d(gen);
        };

    auto filter_rdd = FilterRdd(prev, sample);
    for (const concepts::Split auto& prev_split : filter_rdd) {
      splits_.emplace_back(prev_split, prev_split);
      splits_.back().addDependency(prev_split);
    }

    };

    auto begin() const { return  splits_.begin(); }
    auto end() const { return splits_.end(); }

private:
    using SampleViewType = std::ranges::range_value_t<R>;
    std::vector<ViewSplit<SampleViewType>> splits_{};
    double probability_;
    std::function<bool()> sample;
   
};

}  // namespace cpark

#endif  // CPARK_SAMPLE_RDD_H

