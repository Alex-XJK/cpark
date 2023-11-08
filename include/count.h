#ifndef CPARK_COUNT_H
#define CPARK_COUNT_H

#include <future>
#include <vector>
#include <numeric>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
 * A class who calculate the total count of Rdd.
 */
class Count {
public:
  /** Initialize the Count class. */
  explicit Count() = default;

  /** Compute the count of the Rdd elements. */
  template <concepts::Rdd R>
  unsigned long long operator()(const R& rdd) const {
    // Parallel compute the result.
    std::vector<std::future<unsigned long long>> futures{};
    for (const concepts::Split auto& split : rdd)
      futures.emplace_back(std::async([this, &split]() {
        // TODO: use size() things, instead of manually count the size.
        unsigned long long sCount_ = 0;
        for (const auto& s : split)
          sCount_ += 1;
        return sCount_;
      }));

    auto results =
        std::ranges::subrange(futures) |
        std::views::transform([](std::future<unsigned long long>& fut) { return fut.get(); });

    return std::accumulate(results.begin(), results.end(), 0);
  }
};

/**
 * Helper function to create Count Rdd action with pipeline operator `|`.
 */
template <concepts::Rdd R>
auto operator|(const R& r, const Count& count) {
  return count(r);
}

}  // namespace cpark

#endif  //CPARK_COUNT_H
