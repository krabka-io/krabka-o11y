use super::*;

/// Rejects a range query whose resolution exceeds the per-timeseries point cap.
///
/// Prometheus applies this cap in `web/api/v1/api.go` for every query, and no
/// configured per-tenant limit changes it. Prometheus rejects the query when
/// `(end - start) / step > maxResolution`, with integer division, where
/// `maxResolution` is [`MAX_RESOLUTION_POINTS`]. This function matches the error
/// message and the comma-formatted bound byte-for-byte, so Prometheus and
/// Grafana clients that string-match on the message behave the same.
/// [`duration_param`] has already checked that `step` is positive.
pub(crate) fn check_range_resolution(
    start_ms: i64,
    end_ms: i64,
    step: Time,
) -> Result<(), ApiError> {
    let step_ms = step.millis_i64();
    if step_ms <= 0 {
        return Ok(());
    }
    let intervals = end_ms.saturating_sub(start_ms) / step_ms;
    if intervals > i64::try_from(MAX_RESOLUTION_POINTS).unwrap_or(i64::MAX) {
        return Err(ApiError::bad_data(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)",
        ));
    }
    Ok(())
}
