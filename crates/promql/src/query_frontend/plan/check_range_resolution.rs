use super::{PromqlError, Time, TimeExt};
use crate::engine::range_resolution_exceeds_limit;

/// Rejects a range query whose resolution exceeds the per-timeseries point cap.
///
/// The check matches Prometheus's unconditional front-gate:
/// `(end - start) / step > maxResolution`, with integer division, where
/// `maxResolution` is [`MAX_RESOLUTION_POINTS`]. It runs before the per-step
/// fan-out, so an abusive resolution errors instead of an expansion into ~1e11
/// sub-queries. [`plan_range_query`] already validates that `step` is positive.
pub(crate) fn check_range_resolution(
    start_ms: i64,
    end_ms: i64,
    step: Time,
) -> Result<(), PromqlError> {
    let step_ms = step.millis_i64();
    if range_resolution_exceeds_limit(start_ms, end_ms, step_ms) {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .into(),
        ));
    }
    Ok(())
}
