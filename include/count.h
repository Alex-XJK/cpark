#ifndef CPARK_COUNT_H
#define CPARK_COUNT_H

#include <iostream>

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
  template <concepts::Rdd R, typename T = utils::RddElementType<R>>
  unsigned long long operator()(const R& rdd) const {

    unsigned long long count_ = 0;

    // Assign to different thread
    for (const auto& split : rdd) {
      // count_ += split.size();
      for (const auto& elem : split)
        count_ += 1;
      std::cout << "Total : " << count_ << std::endl;
    }

    return count_;
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
