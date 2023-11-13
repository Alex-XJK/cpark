#include <concepts>
#include <ranges>

#include "base_rdd.h"
#include "utils.h"

namespace cpark {

template <concepts::Rdd R, typename Partitioner = std::hash<utils::RddElementType<R>>>
requires concepts::KeyValueRdd<R> class GroupByKeyRdd {
public:
  explicit
private:
  
};

}  // namespace cpark