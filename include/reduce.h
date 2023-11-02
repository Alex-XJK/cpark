#ifndef CPARK_REDUCE_H
#define CPARK_REDUCE_H

#include <future>
#include <numeric>

#include "base_rdd.h"

namespace cpark {

template <typename Func>
class Reduce {
public:
  explicit Reduce(Func func) : func_{std::move(func)} {}

  template <Rdd R, typename T = std::ranges::range_value_t<R>>
  requires std::invocable<Func, T, T>&& std::convertible_to<std::invoke_result_t<Func, T, T>, T>&&
      std::is_default_constructible_v<T>
          T operator()(const R& r) const {
    std::vector<std::future<T>> futures{};
    for (size_t i : std::views::iota(size_t{0}, r.splits_num())) {
      futures.emplace_back(std::async([this, &r, &futures, i]() {
        auto split = r.get_split(i);
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

template <typename Func, Rdd R>
auto operator|(const R& r, const Reduce<Func>& reduce) {
  return reduce(r);
}

}  // namespace cpark

#endif //CPARK_REDUCE_H
