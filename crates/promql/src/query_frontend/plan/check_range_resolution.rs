use super::{MAX_RESOLUTION_POINTS, PromqlError, Time, TimeExt};

/// Rejects a range query whose resolution exceeds the per-timeseries point cap.
///
/// The check matches Prometheus's unconditional front-gate:
/// `(end - start) / step > maxResolution`, with integer division, where
/// `maxResolution` is [`MAX_RESOLUTION_POINTS`]. It runs before the per-step
/// fan-out, so an abusive resolution errors instead of an expansion into ~1e11
/// sub-queries. [`plan_range_query`] already validates that `step` is positive.
pub(crate) fn check_range_resolution(start_ms: i64, end_ms: i64, step: Time) -> Result<(), PromqlError> {
    let step_ms = step.millis_i64();
    if step_ms <= 0 {
        return Ok(());
    }
    let intervals = end_ms.saturating_sub(start_ms) / step_ms;
    if intervals > i64::try_from(MAX_RESOLUTION_POINTS).unwrap_or(i64::MAX) {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .into(),
        ));
    }
    Ok(())
}
