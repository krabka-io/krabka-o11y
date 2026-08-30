use super::{
    MetricScalarArithmeticOp, MetricValue, Value, apply_metric_scalar_arithmetic_to_sample,
};

pub(crate) fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_arithmetic_to_sample(
                &mut values[index],
                op,
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
    apply_metric_scalar_arithmetic_to_sample(sample, op, scalar, scalar_on_left)
}
