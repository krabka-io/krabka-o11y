use super::ToPrimitive;

/// Converts a `u64` to the nearest `f64`, as Go's `float64(uint64)` does.
pub fn u64_to_f64(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}
