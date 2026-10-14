#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    ComparisonResult, MetricValue, ScalarComparison, Value, json, metric_scalar_comparison_matches,
    scalar_operands_for_sample,
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
    let Some((slot, operands)) = scalar_operands_for_sample(sample, scalar, comparison.scalar_side)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(operands, comparison.comparison.op);
    match comparison.comparison.result {
        ComparisonResult::Bool => {
            *slot = json!(if matches { "1" } else { "0" });
            true
        }
        ComparisonResult::Filter => matches,
    }
}
