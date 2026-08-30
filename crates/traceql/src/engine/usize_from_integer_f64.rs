use super::*;

pub(crate) fn usize_from_integer_f64(value: f64) -> Result<usize> {
    // No finiteness clause: `fract()` of an infinity or a NaN is NaN, and
    // `NaN != 0.0` holds, so the fractional test already refuses every
    // non-finite value. A separate `is_finite` test is unreachable.
    if value < 0.0 || value.fract() != 0.0 {
        return Err(TraceqlError::Exec(format!(
            "expected non-negative integer float, got {value}"
        )));
    }
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseIntError| TraceqlError::Exec(e.to_string()))
}
