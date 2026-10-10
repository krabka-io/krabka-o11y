#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    ComparisonResult, MetricValue, SampleValueSlot, ScalarComparison, ScalarOperands, Value, json,
    metric_scalar_comparison_matches, sample_value_slot,
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
    let Some(SampleValueSlot { slot, sample_value }) = sample_value_slot(sample) else {
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
            *slot = json!(if matches { "1" } else { "0" });
            true
        }
        ComparisonResult::Filter => matches,
    }
}
