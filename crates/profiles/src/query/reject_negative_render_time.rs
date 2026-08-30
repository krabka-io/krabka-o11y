use super::ProfileError;

/// Rejects a render time bound that resolved to a negative millisecond value.
///
/// A `now-<offset>` larger than `now` gives a negative bound, for example after
/// clock skew or a very long lookback. A literal negative timestamp gives one
/// too. A negative bound is never a valid Unix time, and without this check it
/// travels downstream as a query window edge.
pub(crate) fn reject_negative_render_time(
    resolved_ms: i64,
    raw: &str,
) -> Result<i64, ProfileError> {
    if resolved_ms < 0 {
        return Err(ProfileError::Plan(format!(
            "render time {raw:?} resolves to a negative timestamp"
        )));
    }
    Ok(resolved_ms)
}
