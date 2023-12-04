#ifndef CPARK_COLLECT_H
#define CPARK_COLLECT_H

#include <future>
#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/** @ingroup g_acts
 *  @defgroup a_Collect Action >> Collect
 *  This works as the Collect Action of our cpark library
 *  @see Collect
 *  @{
 */

/**
 * A class that collects all the partitions in an Rdd into a single vector.
 */
class Collect {
public:
    /** Initialize the Collect class*/
    explicit Collect() = default;
    /** Collect the elements of the Rdd `rdd` into a vector. */
    template <concepts::Rdd R, typename T = utils::RddElementType<R>>
    std::vector<T> operator()(const R& rdd) const {
    std::vector<std::future<std::vector<T>>> futures{};
    for (const auto& split : rdd) {
      futures.emplace_back(std::async([this, &split]() {
        return std::vector<T>(std::ranges::begin(split), std::ranges::end(split));
      }));
    }
    std::vector<T> result{};
    for (auto& fut : futures) {
      auto split_result = fut.get();
      result.insert(result.end(), split_result.begin(), split_result.end());
    }
    return result;
  }
};

/**
 * Helper function to collect Rdd elements with pipeline operator `|`.
 */
template <concepts::Rdd R>
auto operator|(const R& r, const Collect& collect) {
  return collect(r);
}

/**
 * @example collect_partitions.cpp
 * This is an example use case of
 * collecting all the elements into a vector data structure in the end.
 */

/** @} */ // end of a_Collect

}  // namespace cpark

#endif  // CPARK_COLLECT_H
