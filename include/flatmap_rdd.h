#ifndef CPARK_FLATMAP_RDD_H
#define CPARK_FLATMAP_RDD_H

#include <vector>
#include <ranges>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

template <concepts::Rdd R, typename Func>
requires std::invocable<Func, utils::RddElementType<R>> class FlatMapRdd 
    : public BaseRdd<FlatMapRdd<R, Func>> {
public:
    using Base = BaseRdd<FlatMapRdd<R, Func>>;
    friend Base;

    using OriginalElementType = utils::RddElementType<R>;
    using FlatMapReturnType = std::invoke_result_t<Func, OriginalElementType>;
    using ResultElementType = std::ranges::range_value_t<FlatMapReturnType>;

    class Iterator : public std::forward_iterator_tag {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = ResultElementType;

        using OriginalIterator = std::ranges::iterator_t<std::ranges::range_value_t<R>>;
        using OriginalSentinel = std::ranges::sentinel_t<std::ranges::range_value_t<R>>;
        using FlatMapIterator = std::ranges::iterator_t<FlatMapReturnType>;

        Iterator(OriginalIterator orig_iterator, OriginalSentinel orig_sentinel, Func* func)
            : orig_iterator_{std::move(orig_iterator)}, orig_sentinel_{std::move(orig_sentinel)}, func_{func} {
            advanceToNextValid();
        }

        value_type operator*() const {
            return *flatmap_iterator_;
        }

        Iterator& operator++() {
            ++flatmap_iterator_;
            if (flatmap_iterator_ == std::ranges::end(current_flatmap_result_)) {
                ++orig_iterator_;
                advanceToNextValid();
            }
            return *this;
        }

        Iterator operator++(int) {
            auto old = *this;
            ++(*this);
            return old;
        }

        bool operator==(const Iterator& other) const {
            return orig_iterator_ == other.orig_iterator_ && flatmap_iterator_ == other.flatmap_iterator_;
        }

        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

    private:
        void advanceToNextValid() {
            while (orig_iterator_ != orig_sentinel_) {
                current_flatmap_result_ = (*func_)(*orig_iterator_);
                flatmap_iterator_ = std::ranges::begin(current_flatmap_result_);
                if (flatmap_iterator_ != std::ranges::end(current_flatmap_result_)) {
                    break;
                }
                ++orig_iterator_;
            }
        }

        OriginalIterator orig_iterator_;
        OriginalSentinel orig_sentinel_;
        Func* func_;
        FlatMapReturnType current_flatmap_result_;
        FlatMapIterator flatmap_iterator_;
    };

    FlatMapRdd(const R& prev, Func func) : Base{prev, false}, func_{std::move(func)} {
        static_assert(concepts::Rdd<FlatMapRdd<R, Func>>, "FlatMapRdd does not satisfy Rdd concept.");
        for (const concepts::Split auto& prev_split : prev) {
            splits_.emplace_back(
                std::ranges::subrange{
                    Iterator{std::ranges::begin(prev_split), std::ranges::end(prev_split), &func_},
                    Iterator{std::ranges::end(prev_split), std::ranges::end(prev_split), &func_}},
                prev_split);
            splits_.back().addDependency(prev_split);
        }
    }

    FlatMapRdd(const FlatMapRdd&) = default;
    FlatMapRdd& operator=(const FlatMapRdd&) = default;

private:
    constexpr auto beginImpl() const { return std::ranges::begin(splits_); }
    constexpr auto endImpl() const { return std::ranges::end(splits_); }

private:
    std::vector<ViewSplit<std::ranges::subrange<Iterator>>> splits_{};
    Func func_;
};

template <typename Func>
class FlatMap {
public:
    explicit FlatMap(Func func) : func_{std::move(func)} {}

    template <concepts::Rdd R>
    auto operator()(const R& r) const {
        return FlatMapRdd(r, func_);
    }

private:
    Func func_;
};

template <typename Func, concepts::Rdd R>
auto operator|(const R& r, const FlatMap<Func>& flatMap) {
    return flatMap(r);
}

} // namespace cpark

#endif // CPARK_FLATMAP_RDD_H
