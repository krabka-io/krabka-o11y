use super::{MetricValue, ScalarArithmetic, Value, apply_metric_scalar_arithmetic_to_sample};

pub(crate) fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_arithmetic_to_sample(&mut values[index], arithmetic, scalar) {
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
    apply_metric_scalar_arithmetic_to_sample(sample, arithmetic, scalar)
}
