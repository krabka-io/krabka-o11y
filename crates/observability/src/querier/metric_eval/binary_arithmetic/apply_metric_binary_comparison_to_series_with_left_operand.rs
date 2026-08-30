use super::{
    ComparisonOp, Value, apply_metric_binary_comparison_to_sample_operands,
    matching_metric_binary_sample,
};

pub(crate) fn apply_metric_binary_comparison_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_comparison_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
                bool_modifier,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_comparison_to_sample_operands(
        output_sample,
        left_sample,
        &right_sample,
        op,
        bool_modifier,
    )
}
