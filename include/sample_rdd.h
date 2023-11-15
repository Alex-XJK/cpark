#ifndef CPARK_SAMPLE_RDD_H
#define CPARK_SAMPLE_RDD_H

#include <random>
#include "filter_rdd.h"

namespace cpark {

template <concepts::Rdd R>
class SampleRdd {
public:
    using ElementType = std::ranges::range_value_t<R>;

    SampleRdd(const R& prev, double probability) 
        : sampled_rdd_(FilterRdd(prev, [probability](const ElementType&) {
            static std::random_device rd;
            static std::mt19937 gen(rd());
            std::bernoulli_distribution d(probability);
            return d(gen);
        })) {}

    auto begin() const { return sampled_rdd_.begin(); }
    auto end() const { return sampled_rdd_.end(); }

private:
    FilterRdd<R, std::function<bool(const ElementType&)>> sampled_rdd_;
};

}  // namespace cpark

#endif  // CPARK_SAMPLE_RDD_H
