#ifndef CPARK_COUNT_H
#define CPARK_COUNT_H

#include <future>
#include <vector>
#include <numeric>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/** @defgroup a_Count The Count Action
 *  This works as the Count Action of our cpark library
 *  @see Count
 *  @{
 */

/**
 * A class who counts the total number of elements in parallel.
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
         return static_cast<unsigned long long>(split.size());
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

/** @} */ // end of a_Count

}  // namespace cpark

#endif  //CPARK_COUNT_H
