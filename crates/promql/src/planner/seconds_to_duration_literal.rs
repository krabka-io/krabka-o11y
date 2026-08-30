use super::*;

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
