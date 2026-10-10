use super::{
    MetricComparison, SampleOperands, Value, apply_metric_binary_comparison_to_sample_operands,
};

pub(crate) fn apply_metric_binary_comparison_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    comparison: MetricComparison,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_comparison_to_sample_operands(
        left_sample,
        SampleOperands {
            left: &original_left,
            right: right_sample,
        },
        comparison,
    )
}
