#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    MetricValue, ScalarComparison, Value, apply_scalar_comparison_to_sample, apply_scalar_to_series,
};

#[cfg(test)]
pub(crate) fn apply_metric_scalar_comparison_to_series(
    series: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    apply_scalar_comparison_to_series(series, ScalarComparison::from(comparison), scalar)
}

pub(crate) fn apply_scalar_comparison_to_series(
    series: &mut Value,
    comparison: ScalarComparison,
    scalar: MetricValue,
) -> bool {
    apply_scalar_to_series(series, |sample| {
        apply_scalar_comparison_to_sample(sample, comparison, scalar)
    })
}
