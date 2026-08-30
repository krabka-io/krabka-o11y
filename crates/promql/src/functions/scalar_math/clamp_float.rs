
/// Port of the interpreter's `clamp_float`.
pub(crate) fn clamp_float(value: f64, min: Option<f64>, max: Option<f64>) -> f64 {
    if min.is_some_and(f64::is_nan) || max.is_some_and(f64::is_nan) {
        return f64::NAN;
    }
    if let Some(min) = min
        && value < min
    {
        return min;
    }
    if let Some(max) = max
        && value > max
    {
        return max;
    }
    value
}
