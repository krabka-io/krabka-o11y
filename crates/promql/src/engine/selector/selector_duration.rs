use super::*;

/// A `PromQL` duration literal as a time extent.
///
/// The literal is a form such as `5m`, `1h`, or the `[…]` of a matrix selector.
/// The `i64`-millisecond round trip is the range check, not a unit conversion.
/// This function rejects a literal wider than [`i64::MAX`] milliseconds here,
/// instead of a silent loss of precision downstream, where the caller applies
/// the extent to millisecond instants.
pub(crate) fn selector_duration(duration: std::time::Duration) -> Result<Time> {
    i64::try_from(duration.as_millis())
        .map(Time::from_millis)
        .map_err(|_| PromqlError::Plan("range selector duration is too large".to_string()))
}
