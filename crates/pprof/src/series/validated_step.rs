use super::*;

/// Checks the same bounds as [`step_from_secs`] on an already-typed step.
///
/// A query then cannot reach the bucket arithmetic with a step of zero.
pub(crate) fn validated_step(step: Time) -> Result<Time, ProfileError> {
    let step_secs = step.secs_f64();
    if !(step_secs.is_finite() && step_secs > 0.0) {
        return Err(ProfileError::Plan(format!(
            "step must be a positive finite number of seconds, got {step_secs}"
        )));
    }
    if step < millis(1) {
        return Err(ProfileError::Plan("step must be >= 1ms".to_string()));
    }
    // `millis_i64` saturates, so a step beyond `i64::MAX` milliseconds would
    // silently bucket at the saturated value rather than fail.
    if step >= Time::from_millis(i64::MAX) {
        return Err(ProfileError::Plan(format!(
            "step is too large: {step_secs}"
        )));
    }
    Ok(step)
}
