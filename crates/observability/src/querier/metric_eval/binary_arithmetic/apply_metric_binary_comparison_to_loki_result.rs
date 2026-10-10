use super::{
    MetricBinaryOperator, MetricComparison, MetricVectorMatching, Value,
    apply_metric_binary_comparison_to_series,
    apply_metric_binary_comparison_to_series_with_left_operand,
};

/// A comparison between two vectors, and how their series pair up.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VectorComparison<'a> {
    pub(crate) comparison: MetricComparison,
    pub(crate) matching: Option<&'a MetricVectorMatching>,
}

pub(crate) fn apply_metric_binary_comparison_to_loki_result(
    left: &mut Value,
    right: &Value,
    vector_comparison: VectorComparison<'_>,
) {
    let VectorComparison {
        comparison,
        matching,
    } = vector_comparison;
    MetricBinaryOperator {
        matching,
        apply_series: |left_series: &mut Value, right_series: &Value| {
            apply_metric_binary_comparison_to_series(left_series, right_series, comparison)
        },
        apply_series_with_left_operand: |output_series: &mut Value, left_series: &Value| {
            apply_metric_binary_comparison_to_series_with_left_operand(
                output_series,
                left_series,
                comparison,
            )
        },
    }
    .apply_to_loki_result(left, right);
}
