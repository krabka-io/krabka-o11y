use super::{Duration, PromqlError, Result, ToPrimitive, display_duration};

pub(crate) fn ms_to_seconds(ms: i64) -> f64 {
    ms.to_f64()
        .expect("every i64 has a finite f64 representation")
        / 1000.0
}

pub(crate) fn duration_unit_seconds(unit: &str) -> Result<f64> {
    match unit {
        "ms" => Ok(0.001),
        "s" => Ok(1.0),
        "m" => Ok(60.0),
        "h" => Ok(60.0 * 60.0),
        "d" => Ok(60.0 * 60.0 * 24.0),
        "w" => Ok(60.0 * 60.0 * 24.0 * 7.0),
        "y" => Ok(60.0 * 60.0 * 24.0 * 365.0),
        _ => Err(PromqlError::Parse(format!(
            "invalid duration expression unit `{unit}`"
        ))),
    }
}

/// Largest duration the engine represents: `i64::MAX` milliseconds in seconds.
///
/// `Duration::from_secs_f64` panics for finite values beyond `u64` seconds
/// (~1.8e19), so this function rejects a value past the engine ceiling first.
pub(crate) fn seconds_to_duration_literal(seconds: f64) -> Result<String> {
    let max_duration_seconds = i64::MAX
        .to_f64()
        .expect("i64::MAX has a finite f64 representation")
        / 1000.0;
    if !seconds.is_finite() || !(0.0..=max_duration_seconds).contains(&seconds) {
        return Err(PromqlError::Parse(format!(
            "duration expression evaluated to invalid duration `{seconds}`"
        )));
    }
    let duration = Duration::from_secs_f64(seconds);
    Ok(display_duration(&duration))
}
