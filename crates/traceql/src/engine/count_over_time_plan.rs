use super::{MetricFunction, MetricPlan};

/// Builds a bare `count_over_time()` `MetricPlan`: no grouping, no stages, no
/// sampling, and no `compare()` stage.
pub(crate) fn count_over_time_plan() -> MetricPlan {
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
        compare: None,
    }
}
