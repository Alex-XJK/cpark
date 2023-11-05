#ifndef CPARK_FILTER_RDD_H
#define CPARK_FILTER_RDD_H

#include <vector>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

/**
* An Rdd holding the data filtered from an old rdd by some function.
* @tparam R Type of the old Rdd.
* @tparam Func Type of the transformation function.
* The type of the old Rdd `R`'s elements should be able to invoke function `Func`.
*/
    template <concepts::Rdd R, typename Func>
    requires
        std::invocable<Func, utils::RddElementType<R> > &&
        std::is_same_v<std::invoke_result_t<Func, utils::RddElementType<R>>, bool>
    class FilterRdd : public BaseRdd<FilterRdd<R, Func>> {
    public:
        using Base = BaseRdd<FilterRdd<R, Func>>;
        friend Base;

        constexpr FilterRdd(const R& prev, Func func) : Base{prev, false} {
            static_assert(concepts::Rdd<FilterRdd<R, Func>>, "Instance of FilterRdd does not satisfy Rdd concept.");
            // Create the transformed splits.
            for (const concepts::Split auto& prev_split : prev) {
                splits_.emplace_back(prev_split | std::views::filter(func), prev_split);
                splits_.back().addDependency(prev_split);
            }
        }

        // Explicitly define default copy constrictor and assignment operator,
        // because some linters or compilers can not define implicit copy constructors for this class,
        // though they are supposed to do so.
        // TODO: find out why.
        constexpr FilterRdd(const FilterRdd&) = default;
        FilterRdd& operator=(const FilterRdd&) = default;

    private:
        constexpr auto beginImpl() const { return std::ranges::begin(splits_); }

        constexpr auto endImpl() const { return std::ranges::end(splits_); }

    private:
        using FilterViewype = decltype(std::declval<R>().front() | std::views::filter(std::declval<Func>()));
        // A vector holding the splits for this rdd.
        std::vector<ViewSplit<FilterViewype>> splits_{};
    };

/**
 * Helper class to create Transformed Rdd with pipeline operator `|`.
 */
    template <typename Func>
    class Filter {
    public:
        explicit Filter(Func func) : func_{std::move(func)} {}

        template <concepts::Rdd R, typename T = utils::RddElementType<R> >
        requires std::invocable<Func, T>&& std::is_same_v<std::invoke_result_t<Func, T>, bool> auto
        operator()(const R& r) const {
            return FilterRdd(r, func_);
        }

    private:
        Func func_;
    };

/**
 * Helper function to create Filter Rdd with pipeline operator `|`.
 */
    template <typename Func, concepts::Rdd R>
    auto operator|(const R& r, const Filter<Func>& filter) {
        return filter(r);
    }

}  // namespace cpark

#endif  //CPARK_FILTER_RDD_H
