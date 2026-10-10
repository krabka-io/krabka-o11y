use super::{
    ComparisonOp, SampleOperands, Value, json, metric_binary_operand_values,
    metric_scalar_comparison_matches,
};

pub(crate) fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some((left_value, right_value)) = metric_binary_operand_values(SampleOperands {
        left: left_sample,
        right: right_sample,
    }) else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(left_value, op, right_value, false);
    if bool_modifier {
        if let Some(value) = output_values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        if matches
            && let (Some(output), Some(left)) = (output_values.get_mut(1), left_sample.get(1))
        {
            *output = left.clone();
        }
        matches
    }
}
