use super::{
    ComparisonOp, Value, apply_metric_binary_comparison_to_sample_operands,
    apply_metric_binary_to_series_with_left_operand,
};

pub(crate) fn apply_metric_binary_comparison_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    apply_metric_binary_to_series_with_left_operand(
        output_series,
        left_series,
        |output_sample, left_sample, right_sample| {
            apply_metric_binary_comparison_to_sample_operands(
                output_sample,
                left_sample,
                right_sample,
                op,
                bool_modifier,
            )
        },
    )
}
