#[cfg(test)]
use super::MetricScalarComparison;
use super::{MetricValue, ScalarComparison, Value, apply_scalar_comparison_to_sample};

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
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_scalar_comparison_to_sample(&mut values[index], comparison, scalar) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_scalar_comparison_to_sample(sample, comparison, scalar)
}
