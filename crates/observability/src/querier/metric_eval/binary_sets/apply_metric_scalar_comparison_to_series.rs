use super::{MetricScalarComparison, MetricValue, Value, apply_scalar_comparison_to_sample};

pub(crate) fn apply_metric_scalar_comparison_to_series(
    series: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    apply_scalar_comparison_to_series(
        series,
        comparison.op,
        comparison.bool_modifier,
        scalar,
        comparison.scalar_on_left,
    )
}

pub(crate) fn apply_scalar_comparison_to_series(
    series: &mut Value,
    op: crate::ComparisonOp,
    bool_modifier: bool,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_scalar_comparison_to_sample(
                &mut values[index],
                op,
                bool_modifier,
                scalar,
                scalar_on_left,
            ) {
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
    apply_scalar_comparison_to_sample(sample, op, bool_modifier, scalar, scalar_on_left)
}
