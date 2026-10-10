/// Returns the sign of `value` as the `PromQL` `sgn` function does: `1`, `-1` or `0`, and
/// NaN for NaN.
pub(crate) fn prometheus_sgn(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value > 0.0 {
        1.0
    } else if value < 0.0 {
        -1.0
    } else {
        0.0
    }
}
