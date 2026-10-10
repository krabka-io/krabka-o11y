use super::{
    MetricComparison, Value, apply_metric_binary_comparison_to_sample,
    apply_metric_binary_to_series,
};

pub(crate) fn apply_metric_binary_comparison_to_series(
    left_series: &mut Value,
    right_series: &Value,
    comparison: MetricComparison,
) -> bool {
    apply_metric_binary_to_series(left_series, right_series, |left_sample, right_sample| {
        apply_metric_binary_comparison_to_sample(left_sample, right_sample, comparison)
    })
}
