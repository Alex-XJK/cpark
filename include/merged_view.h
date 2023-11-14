#include <concepts>
#include <ranges>
#include <vector>

namespace concepts {
template <typename R>
concept RangeOfView =
    std::ranges::forward_range<R> && std::ranges::view<std::ranges::range_value_t<R>>;
}  // namespace concepts

/**
 * A view combined from any number of views of the same type.
 * It contains all the elements in these views with their original order.
 * @tparam R A range of views to be combined.
 */
template <concepts::RangeOfView R>
class MergedSameView : public std::ranges::view_interface<MergedSameView<R>> {
public:
  using ViewType = std::ranges::range_value_t<R>;

  /**
   * An lazily evaluated iterator that sequentially reads the elements in a
   * sequence of views.
   */
  class Iterator : public std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = std::ranges::range_value_t<ViewType>;

  public:
    /**
     * Initialize the Iterator with:
     * @param start_iterator The iterator to the first element.
     * @param views A pointer to a vector of of views to be combined.
     * @param start_index The index of the first view.
     */
    Iterator(std::ranges::iterator_t<const ViewType> start_iterator, std::vector<ViewType>* views,
             std::vector<ViewType>::size_type start_index)
        : inner_iterator_{std::move(start_iterator)}, views_{views}, index_of_view_{start_index} {
      // Move the inner iterator to the first element which is not an 'end'.
      while (index_of_view_ < std::ranges::size(*views_) &&
             inner_iterator_ == std::ranges::end((*views_)[index_of_view_])) {
        ++index_of_view_;
        if (index_of_view_ < std::ranges::size(*views_)) {
          inner_iterator_ = std::ranges::begin((*views_)[index_of_view_]);
        }
      }
    }

    Iterator() = default;

    value_type operator*() const { return *inner_iterator_; }

    Iterator& operator++() {
      ++inner_iterator_;
      // Move the inner iterator to the first element which is not an 'end'.
      while (index_of_view_ < std::ranges::size(*views_) &&
             inner_iterator_ == std::ranges::end((*views_)[index_of_view_])) {
        ++index_of_view_;
        if (index_of_view_ < std::ranges::size(*views_)) {
          inner_iterator_ = std::ranges::begin((*views_)[index_of_view_]);
        }
      }
      return *this;
    }

    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    /**
     * Two Iterator-s are equal iff:
     * They are both 'end' iterators, or if they have the same view sequence and point to the same
     * element.
     */
    bool operator==(const Iterator& other) const {
      if (isEnd() && other.isEnd()) {
        return true;
      }
      return views_ == other.views_ && inner_iterator_ == other.inner_iterator_ &&
             index_of_view_ == other.index_of_view_;
    }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

    Iterator& operator--()
      requires std::ranges::bidirectional_range<ViewType>
    {}

  private:
    bool isEnd() const { return !(index_of_view_ < std::ranges::size(*views_)); }

  private:
    // Iterator pointing to the current element.
    std::ranges::iterator_t<const ViewType> inner_iterator_{};
    // The sequence of views to be combined.
    std::vector<ViewType>* views_{nullptr};
    // Index of the view that has the current `inner_iterator_`.
    std::vector<ViewType>::size_type index_of_view_{0};
  };

public:
  /**
   * Creates MergedSameView from a range of views to be combined.
   */
  explicit MergedSameView(const R& views)
      : views_{std::ranges::begin(views), std::ranges::end(views)} {}

  /** The iterator to the first element of the combined views. */
  constexpr auto begin() const {
    auto start_iterator = std::ranges::empty(views_) ? std::ranges::iterator_t<ViewType>{}
                                                     : std::ranges::begin(views_.front());
    return Iterator{start_iterator, const_cast<std::vector<ViewType>*>(&views_),
                    static_cast<std::vector<ViewType>::size_type>(0)};
  }

  /** The sentinel pass the last element of the combined views. */
  constexpr auto end() const {
    return Iterator{std::ranges::iterator_t<ViewType>{},
                    const_cast<std::vector<ViewType>*>(&views_), std::ranges::size(views_)};
  }

private:
  std::vector<ViewType> views_;
};

/**
 * A helper class that combines two views of different types into one.
 * Their element types must be the same.
 */
template <std::ranges::view V1, std::ranges::view V2>
  requires std::same_as<std::ranges::range_value_t<V1>, std::ranges::range_value_t<V2>>
class MergedTwoDiffView : public std::ranges::view_interface<MergedTwoDiffView<V1, V2>> {
public:
  /**
   * A lazily evaluated iterator that read elements combined from two different views.
   */
  class Iterator : public std::forward_iterator_tag {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = std::ranges::range_value_t<V1>;

  private:
    // Types of the iterators of the two original views.
    using OriginalIterator1 = std::ranges::iterator_t<const V1>;
    using OriginalIterator2 = std::ranges::iterator_t<const V2>;

  public:
    /**
    * Creates Iterator with:
    * @tparam Iter Type of the start iterator.
    * @param iterator The start iterator points to the first element.
    * @param view1 The first view.
    * @param view2 The second view.
    * @param if_hold1 Whether the start iterator is from view1 or view2.
    * Note that `if_hold1` must be consistent with the type of `Iter`.
    */
    template <typename Iter>
    Iterator(Iter iterator, V1* view1, V2* view2, bool if_hold1)
        : hold1_{if_hold1}, view1_{view1}, view2_{view2} {
      // If two inner iterator types are the same, use hold1() to determine which iterator to be
      // assigned.
      if constexpr (std::same_as<OriginalIterator1, OriginalIterator2>) {
        if (hold1()) {
          get1() = iterator;
        } else {
          get2() = iterator;
        }
      } else {
        if constexpr (std::convertible_to<Iter, OriginalIterator1>) {
          get1() = iterator;
        } else {
          get2() = iterator;
        }
      }
    }

    Iterator() = default;

    value_type operator*() const { return hold1() ? *get1() : *get2(); }

    Iterator& operator++() {
      if (hold1())
        ++get1();
      else
        ++get2();
      if (hold1() && get1() == std::ranges::end(*view1_)) {
        get2() = std::ranges::begin(*view2_);
        hold1_ = false;
      }
      return *this;
    }

    Iterator operator++(int) {
      auto old = *this;
      ++(*this);
      return old;
    }

    bool operator==(const Iterator& other) const {
      return (hold1() && other.hold1() && get1() == other.get1()) ||
             (hold2() && other.hold2() && get2() == other.get2());
    }

    bool operator!=(const Iterator& other) const { return !(*this == other); }

  private:
    bool hold1() const { return hold1_; }
    decltype(auto) get1() { return (inner_iterator1_); }
    decltype(auto) get1() const { return (inner_iterator1_); }

    bool hold2() const { return !hold1_; }
    decltype(auto) get2() { return (inner_iterator2_); }
    decltype(auto) get2() const { return (inner_iterator2_); }

  private:
    // In case OriginalIterator1 and OriginalIterator2 are same type, std::variant can not be used.
    OriginalIterator1 inner_iterator1_;
    OriginalIterator2 inner_iterator2_;
    bool hold1_{true};
    V1* view1_{nullptr};
    V2* view2_{nullptr};
  };

public:
  /**
   * Creates MergedTwoDiffView from two views of possibily different types.
   */
  MergedTwoDiffView(V1 view1, V2 view2) : view1_{std::move(view1)}, view2_{std::move(view2)} {}

  /** The iterator to the first element of the combined views. */
  constexpr auto begin() const {
    return Iterator{std::ranges::begin(view1_), const_cast<V1*>(&view1_), const_cast<V2*>(&view2_),
                    true};
  }

  /** The sentinel pass the last element of the combined views. */
  constexpr auto end() const {
    return Iterator{std::ranges::end(view2_), const_cast<V1*>(&view1_), const_cast<V2*>(&view2_),
                    false};
  }

private:
  V1 view1_;
  V2 view2_;
};

template <std::ranges::view FirstView>
auto MergedDiffView(const FirstView& view) {
  return view;
}

/** Creates a combined view from any number of possibly different views. */
template <std::ranges::view FirstView, std::ranges::view... OtherViews>
auto MergedDiffView(const FirstView& first_view, const OtherViews&... other_views) {
  return MergedTwoDiffView{first_view, MergedDiffView(other_views...)};
}
