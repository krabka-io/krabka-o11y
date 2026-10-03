use super::ToPrimitive;

/// Converts an `i64` to the nearest `f64`, as Go's `float64(int64)` does.
pub fn i64_to_f64(value: i64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}
