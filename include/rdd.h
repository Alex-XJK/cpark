#ifndef CPARK_RDD_H
#define CPARK_RDD_H

#include <concepts>
#include <ranges>
#include <memory>
#include <future>
#include <vector>
#include <numeric>

namespace cpark {

/**
 * The most basic concept of RDD.
 * An Rdd is an input_range that has been logically partitioned into several sub parts (split),
 * with a member function get_split() that takes a size_t i and returns its i-th split as an input_range,
 * and a member function splits_num() to get the total number of splits.
 * @tparam R Type to be checked.
 */
template<typename R>
concept Rdd =
std::ranges::input_range<R> && requires(const R &r) {
    { r.get_split(std::declval<size_t>()) } -> std::ranges::input_range;
    { r.splits_num() } -> std::convertible_to<size_t>;
    requires std::same_as<
        std::ranges::range_value_t<decltype(r.get_split(std::declval<size_t>()))>,
        std::ranges::range_value_t<R>>;
};


/**
 * A plain RDD holding the same data from a std::ranges::view without any changes.
 * @tparam R The type of the view containing the original data.
 */
template<std::ranges::view R>
class PlainRdd {
public:
    using iterator = std::ranges::iterator_t<R>;
public:
    constexpr explicit PlainRdd(R view) : view_{std::move(view)} {
        static_assert(Rdd<PlainRdd<R>>, "Instance of PlainRdd does not satisfy Rdd concept.");
        size_ = std::ranges::size(view_);
    }

    constexpr auto get_split(size_t i) const {
        size_t split_size = size_ / splits_;
        auto b = begin(), e = begin();
        std::ranges::advance(b, i * split_size);
        std::ranges::advance(e, std::min(size_, (i + 1) * split_size));
        return std::ranges::subrange(b, e);
    }

    constexpr size_t splits_num() const {
        return splits_;
    }

    constexpr auto begin() const {
        return std::ranges::begin(view_);
    }

    constexpr auto end() const {
        return std::ranges::end(view_);
    }

private:
    size_t splits_{8};
    size_t size_;
    R view_;
};


/**
 * An Rdd that generates a sequence of data from a number range.
 * @tparam Num The type of the number composing the range. It must be arithmetic types.
 * @tparam Func The function that maps the number into some data. Must be invokable from Num.
 * @tparam T The data type of the Rdd. Must be convertible from the result of Func.
 */
template<typename Num, typename Func, typename T = std::invoke_result_t<Func, Num>> requires
std::is_arithmetic_v<Num> &&
std::invocable<Func, Num> && std::convertible_to<std::invoke_result_t<Func, Num>, T>
class GeneratorRdd {
public:
    constexpr GeneratorRdd(Num begin, Num end, Func func) : func_{std::move(func)}, begin_{begin}, end_{end} {
        static_assert(Rdd<GeneratorRdd<Num, Func, T>>, "Instance of GeneratorRdd does not satisfy Rdd concept.");
    }

    class iterator: std::forward_iterator_tag {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = T;

        iterator() = default;

        iterator(const Func* func, Num i) : func_{func}, i_{i} {}

        // Not sure if returning value_type would satisfy input_range requirements. Works for now.
        value_type operator*() const { return (*func_)(i_); }

        iterator &operator++() {
            ++i_;
            return *this;
        }

        iterator operator++(int) {
            auto old = *this;
            ++i_;
            return old;
        }

        bool operator==(const iterator &other) const {
            return func_ == other.func_ && i_ == other.i_;
        }

        bool operator!=(const iterator &other) const {
            return !(*this == other);
        }

    private:
        const Func* func_;
        Num i_{};
    };

    constexpr auto get_split(size_t i) const {
        size_t size = (end_ - begin_);
        size_t split_size = size / splits_;
        auto b = begin(), e = begin();
        std::ranges::advance(b, i * split_size);
        std::ranges::advance(e, std::min(size, (i + 1) * split_size));
        return std::ranges::subrange(b, e);
    }

    constexpr size_t splits_num() const {
        return splits_;
    }

    constexpr iterator begin() const {
        return {&func_, begin_};
    }

    constexpr iterator end() const {
        return {&func_, end_};
    }

private:
    Func func_;
    Num begin_, end_;
    size_t splits_{8};
    // Do we need a cache for the generator?
    std::shared_ptr<std::pair<bool, T>[]> cache_;
};


/**
 * An Rdd holding the data transformed from an old rdd by some function.
 * @tparam R Type of the old Rdd.
 * @tparam Func Type of the transformation function.
 * @tparam T Type of the data hold in this Rdd.
 */
template<Rdd R, typename Func, typename T = std::invoke_result_t<Func, std::ranges::range_value_t<R>>> requires
std::invocable<Func, std::ranges::range_value_t<R>> &&
std::convertible_to<std::invoke_result_t<Func, std::ranges::range_value_t<R>>, T>
class TransformedRdd {
public:
    constexpr TransformedRdd(R prev, Func func) : prev_{std::move(prev)}, func_{std::move(func)},
                                                  split_ranges_(splits_), splits_{prev.splits_num()} {
        static_assert(Rdd<TransformedRdd<R, Func, T>>, "Instance of TransformedRdd does not satisfy Rdd concept.");
        // Create the transformed splits.
        for (size_t i: std::views::iota(size_t{0}, splits_)) {
            split_ranges_[i] = prev_.get_split(i) | std::views::transform(func_);
        }
    }

    constexpr auto get_split(size_t i) const {
        return split_ranges_[i];
    }

    constexpr size_t splits_num() const {
        return splits_;
    }

    constexpr auto begin() const {
        return std::ranges::begin(split_ranges_.front());
    }

    constexpr auto end() const {
        return std::ranges::end(split_ranges_.back());
    }

private:
    R prev_;
    Func func_;
    size_t splits_{};

    using TransformedSplitType = decltype(prev_.get_split(std::declval<size_t>()) | std::views::transform(func_));
    std::vector<TransformedSplitType> split_ranges_;
};

template<typename Func>
class Reduce {
public:
    explicit Reduce(Func func) : func_{std::move(func)} {}

    template<Rdd R, typename T = std::ranges::range_value_t<R>>
    requires std::invocable<Func, T, T> && std::convertible_to<std::invoke_result_t<Func, T, T>, T> &&
             std::is_default_constructible_v<T>
    T operator()(const R &r) const {
        std::vector<std::future<T>> futures{};
        for (size_t i: std::views::iota(size_t{0}, r.splits_num())) {
            futures.emplace_back(std::async([this, &r, &futures, i]() {
                auto split = r.get_split(i);
                return std::reduce(std::ranges::begin(split), std::ranges::end(split), T{}, func_);
            }));
        }
        auto results = std::ranges::subrange(futures) | std::views::transform([](std::future<T> &fut) {
            return fut.get();
        });
        return std::reduce(std::ranges::begin(results), std::ranges::end(results), T{}, func_);
    }

private:
    Func func_;
};

template<typename Func, Rdd R>
auto operator|(const R &r, const Reduce<Func> &reduce) {
    return reduce(r);
}

}   // namespace cpark

#endif // CPARK_RDD_H
