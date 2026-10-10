#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    ComparisonResult, MetricValue, ScalarComparison, ScalarOperands, Value, json,
    metric_scalar_comparison_matches, parse_metric_sample_value,
};

#[cfg(test)]
pub(crate) fn apply_metric_scalar_comparison_to_sample(
    sample: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    apply_scalar_comparison_to_sample(sample, ScalarComparison::from(comparison), scalar)
}

pub(crate) fn apply_scalar_comparison_to_sample(
    sample: &mut Value,
    comparison: ScalarComparison,
    scalar: MetricValue,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let operands = ScalarOperands {
        sample: sample_value,
        scalar,
        scalar_side: comparison.scalar_side,
    };
    let matches = metric_scalar_comparison_matches(operands, comparison.comparison.op);
    match comparison.comparison.result {
        ComparisonResult::Bool => {
            if let Some(value) = values.get_mut(1) {
                *value = json!(if matches { "1" } else { "0" });
            }
            true
        }
        ComparisonResult::Filter => matches,
    }
}
