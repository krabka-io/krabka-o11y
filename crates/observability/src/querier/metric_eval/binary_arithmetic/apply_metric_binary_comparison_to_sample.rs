use super::{ComparisonOp, Value, apply_metric_binary_comparison_to_sample_operands};

pub(crate) fn apply_metric_binary_comparison_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_comparison_to_sample_operands(
        left_sample,
        &original_left,
        right_sample,
        op,
        bool_modifier,
    )
}
