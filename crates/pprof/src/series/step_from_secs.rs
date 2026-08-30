use super::*;

/// Returns the step of a select-series query.
///
/// The Pyroscope `step` query parameter carries the step as fractional seconds.
///
/// # Errors
/// Returns [`ProfileError::Plan`] when the step is not a positive finite number
/// of seconds, is shorter than a millisecond, or is too long to express as whole
/// milliseconds in an `i64`.
pub fn step_from_secs(step_secs: f64) -> Result<Time, ProfileError> {
    validated_step(Time::from_secs_f64(step_secs))
}
