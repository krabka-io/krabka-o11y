use super::*;

/// Builds the `MetricPlan` for a `compare()` stage.
///
/// The `function`, `value`, and `by` fields are inert placeholders.
/// `query_range_compare` reads `compare` directly, and never runs the
/// `*_over_time()` machinery.
pub(crate) fn metric_plan_with_compare(compare: CompareSpec) -> MetricPlan {
    MetricPlan {
        function: MetricFunction::CountOverTime,
        value: None,
        quantiles: Vec::new(),
        by: Vec::new(),
        filter: None,
        rank: None,
        compare: Some(compare),
    }
}
