#ifndef CPARK_REDUCE_H
#define CPARK_REDUCE_H

#include <future>
#include <numeric>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
 * A class who receives a function (callable object) and calculate the reduced result of Rdd using
 * the function.
 */
template <typename Func>
class Reduce {
public:
  /** Initialize the Reduce class with a callable object `func`. */
  explicit Reduce(Func func) : func_{std::move(func)} {}

  /** Compute the result of the Rdd `rdd` with the function. */
  template <concepts::Rdd R, typename T = utils::RddElementType<R>>
  requires std::invocable<Func, T, T>&& std::convertible_to<std::invoke_result_t<Func, T, T>, T>&&
      std::is_default_constructible_v<T>
          T operator()(const R& rdd) const {
    // Parallelly compute the result.
    // TODO: add complete parallel control and sync logic.
    std::vector<std::future<T>> futures{};
    for (const auto& split : rdd) {
      futures.emplace_back(std::async([this, &split]() {
        return std::reduce(std::ranges::begin(split), std::ranges::end(split), T{}, func_);
      }));
    }
    auto results = std::ranges::subrange(futures) |
                   std::views::transform([](std::future<T>& fut) { return fut.get(); });
    return std::reduce(std::ranges::begin(results), std::ranges::end(results), T{}, func_);
  }

private:
  Func func_;
};

/**
 * Helper function to create Reduced Rdd result with pipeline operator `|`.
 */
template <typename Func, concepts::Rdd R>
auto operator|(const R& r, const Reduce<Func>& reduce) {
  return reduce(r);
}

}  // namespace cpark

#endif  //CPARK_REDUCE_H
