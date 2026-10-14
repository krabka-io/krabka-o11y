use super::{
    MetricValue, ScalarArithmetic, Value, apply_metric_scalar_arithmetic_to_sample,
    apply_scalar_to_series,
};

pub(crate) fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
) -> bool {
    apply_scalar_to_series(series, |sample| {
        apply_metric_scalar_arithmetic_to_sample(sample, arithmetic, scalar)
    })
}
