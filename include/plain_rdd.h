#ifndef CPARK_PLAIN_RDD_H
#define CPARK_PLAIN_RDD_H

#include "base_rdd.h"

namespace cpark {

/** @ingroup g_creates
 *  @defgroup c_Plain Plain
 *  This works as a Creation Method to generate cpark Rdd from std::ranges::view
 *  @see PlainRdd
 *  @{
 */

/**
* A plain RDD holding the same data from a std::ranges::view without any
* changes.
* @tparam R The type of the view containing the original data.
*/
template <std::ranges::view R>
class PlainRdd : public BaseRdd<PlainRdd<R>> {
public:
  using Base = BaseRdd<PlainRdd<R>>;
  friend Base;

public:
public:
  constexpr explicit PlainRdd(R view, ExecutionContext* context)
      : BaseRdd<PlainRdd<R>>{context}, view_{std::move(view)} {
    static_assert(concepts::Rdd<PlainRdd<R>>, "Instance of PlainRdd does not satisfy Rdd concept.");
    splits_.reserve(Base::splits_num_);

    // Creates the splits for this Rdd. Each split contains two iterators that points to the values
    // that this split has.
    for (size_t i : std::views::iota(size_t{0}, Base::splits_num_)) {
      size_t total_size = std::ranges::size(view_);
      size_t split_size = (total_size + Base::splits_num_ - 1) / Base::splits_num_;
      auto b = std::ranges::begin(view_), e = std::ranges::begin(view_);
      std::ranges::advance(b, std::min(total_size, i * split_size));
      std::ranges::advance(e, std::min(total_size, (i + 1) * split_size));
      splits_.emplace_back(std::ranges::subrange{b, e}, Base::context_);
    }
  }

  // Explicitly define default copy constrictor and assignment operator,
  // because some linters or compilers can not define implicit copy constructors for this class,
  // though they are supposed to do so.
  // TODO: find out why.
  constexpr PlainRdd(const PlainRdd&) = default;
  PlainRdd& operator=(const PlainRdd&) = default;

private:
  constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

  constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
  R view_;
  // A vector holding the splits for this rdd.
  std::vector<ViewSplit<std::ranges::subrange<std::ranges::iterator_t<R>>>> splits_{};
};

/** @} */ // end of c_Plain

}  // namespace cpark

#endif  //CPARK_PLAIN_RDD_H
