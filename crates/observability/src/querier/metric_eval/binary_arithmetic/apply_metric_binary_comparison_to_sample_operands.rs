use super::{
    ComparisonResult, MetricComparison, SampleOperands, ScalarOperands, ScalarSide, Value, json,
    metric_binary_operand_values, metric_scalar_comparison_matches,
};

pub(crate) fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    operands: SampleOperands<'_>,
    comparison: MetricComparison,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some((left_value, right_value)) = metric_binary_operand_values(operands) else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(
        ScalarOperands {
            sample: left_value,
            scalar: right_value,
            scalar_side: ScalarSide::Right,
        },
        comparison.op,
    );
    match comparison.result {
        ComparisonResult::Bool => {
            if let Some(value) = output_values.get_mut(1) {
                *value = json!(if matches { "1" } else { "0" });
            }
            true
        }
        ComparisonResult::Filter => {
            if matches
                && let (Some(output), Some(left)) = (output_values.get_mut(1), operands.left.get(1))
            {
                *output = left.clone();
            }
            matches
        }
    }
}
