use super::{
    ComparisonResult, MetricComparison, SampleOperands, Value, json,
    metric_scalar_comparison_matches, output_sample_scalar_operands,
};

pub(crate) fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    operands: SampleOperands<'_>,
    comparison: MetricComparison,
) -> bool {
    let Some((output_values, scalar_operands)) =
        output_sample_scalar_operands(output_sample, operands)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(scalar_operands, comparison.op);
    match comparison.result {
        ComparisonResult::Bool => {
            if let Some(value) = output_values.get_mut(1) {
                *value = json!(if matches { "1" } else { "0" });
            }
            true
        }
        ComparisonResult::Filter => {
            if matches && let Some(left) = operands.left.get(1) {
                if let Some(output) = output_values.get_mut(1) {
                    *output = left.clone();
                }
            }
            matches
        }
    }
}
