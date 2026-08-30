use super::MetricFilter;

pub(crate) fn metric_filter_passes(value: f64, filter: MetricFilter) -> bool {
    let ordering = value.total_cmp(&filter.value);
    match filter.op {
        crate::ast::ComparisonOp::Eq => ordering.is_eq(),
        crate::ast::ComparisonOp::Neq => !ordering.is_eq(),
        crate::ast::ComparisonOp::Lt => ordering.is_lt(),
        crate::ast::ComparisonOp::Lte => !ordering.is_gt(),
        crate::ast::ComparisonOp::Gt => ordering.is_gt(),
        crate::ast::ComparisonOp::Gte => !ordering.is_lt(),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => false,
    }
}
