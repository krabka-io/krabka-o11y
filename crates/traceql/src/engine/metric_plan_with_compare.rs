use super::{CompareSpec, MetricPlan, count_over_time_plan};

/// Builds the `MetricPlan` for a `compare()` stage.
///
/// The `function`, `value`, and `by` fields are inert placeholders.
/// `query_range_compare` reads `compare` directly, and never runs the
/// `*_over_time()` machinery.
pub(crate) fn metric_plan_with_compare(compare: CompareSpec) -> MetricPlan {
    MetricPlan {
        compare: Some(compare),
        ..count_over_time_plan()
    }
}
