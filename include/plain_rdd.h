#ifndef CPARK_PLAIN_RDD_H
#define CPARK_PLAIN_RDD_H

#include "base_rdd.h"

namespace cpark {

/**
* A plain RDD holding the same data from a std::ranges::view without any
* changes.
* @tparam R The type of the view containing the original data.
*/
template <std::ranges::view R>
class PlainRdd : public BaseRdd<PlainRdd<R>> {
public:
  friend BaseRdd<PlainRdd<R>>;
  using iterator = std::ranges::iterator_t<R>;

public:
  constexpr explicit PlainRdd(R view) : view_{std::move(view)} {
    static_assert(Rdd<PlainRdd<R>>, "Instance of PlainRdd does not satisfy Rdd concept.");
    size_ = std::ranges::size(view_);
  }

private:
  constexpr auto get_split_impl(size_t i) const {
    size_t split_size = size_ / splits_;
    auto b = begin(), e = begin();
    std::ranges::advance(b, i * split_size);
    std::ranges::advance(e, std::min(size_, (i + 1) * split_size));
    return std::ranges::subrange(b, e);
  }

  constexpr size_t splits_num_impl() const { return splits_; }

public:
  constexpr auto begin() const { return std::ranges::begin(view_); }

  constexpr auto end() const { return std::ranges::end(view_); }

private:
  size_t splits_{8};
  size_t size_;
  R view_;
};

}  // namespace cpark

#endif  //CPARK_PLAIN_RDD_H
