use super::MetricFilter;

pub(crate) fn metric_filter_passes(value: f64, filter: MetricFilter) -> bool {
    let ordering = value.partial_cmp(&filter.value);
    match filter.op {
        crate::ast::ComparisonOp::Eq => ordering.is_some_and(std::cmp::Ordering::is_eq),
        crate::ast::ComparisonOp::Neq => !ordering.is_some_and(std::cmp::Ordering::is_eq),
        crate::ast::ComparisonOp::Lt => ordering.is_some_and(std::cmp::Ordering::is_lt),
        crate::ast::ComparisonOp::Lte => ordering.is_some_and(|ordering| !ordering.is_gt()),
        crate::ast::ComparisonOp::Gt => ordering.is_some_and(std::cmp::Ordering::is_gt),
        crate::ast::ComparisonOp::Gte => ordering.is_some_and(|ordering| !ordering.is_lt()),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => false,
    }
}
