use super::*;

#[derive(Clone, Copy)]
pub(crate) struct MetricFilter {
    pub(crate) op: crate::ast::ComparisonOp,
    pub(crate) value: f64,
}

pub(crate) fn metric_filter(op: crate::ast::ComparisonOp, value: f64) -> Result<MetricFilter> {
    if !value.is_finite() {
        return Err(TraceqlError::Plan(
            "metric comparison filter value is not finite".into(),
        ));
    }
    match op {
        crate::ast::ComparisonOp::Eq
        | crate::ast::ComparisonOp::Neq
        | crate::ast::ComparisonOp::Lt
        | crate::ast::ComparisonOp::Lte
        | crate::ast::ComparisonOp::Gt
        | crate::ast::ComparisonOp::Gte => Ok(MetricFilter { op, value }),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => Err(
            TraceqlError::Unsupported("regex filter on metric scalar is not supported".into()),
        ),
    }
}
