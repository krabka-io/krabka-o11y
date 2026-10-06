use super::{CompareSpec, MetricFunction, MetricPlan};

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
        exemplar_fields: Vec::new(),
        stages: Vec::new(),
        spanset_pipeline: Vec::new(),
        sampling_factor: 1.0,
        spanset_pipeline_had_input: false,
        frontend_labels: false,
        instant: false,
        compare: Some(compare),
    }
}
