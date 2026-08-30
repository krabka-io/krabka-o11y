use super::{
    ComparisonOp, Value, json, metric_binary_sample_timestamps_match,
    metric_scalar_comparison_matches, parse_metric_sample_value,
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
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
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
            && let (Some(output), Some(left)) = (output_values.get_mut(1), left_values.get(1))
        {
            *output = left.clone();
        }
        matches
    }
}
