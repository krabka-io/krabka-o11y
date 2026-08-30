use super::{Time, Result, TimeExt, PromqlError, MAX_RESOLUTION_POINTS};

/// Returns the resolution-point count `(end_ms - start_ms) / step + 1`.
///
/// The count is for a range or subquery grid. This function rejects an abusive
/// resolution before any per-step evaluation runs. It applies the cap to the
/// interval count `(end - start) / step`. That matches the Prometheus
/// `(end-start)/step > 11000` rule, and it matches the HTTP front gate
/// byte-for-byte in error type, status, and message. A query that the gate
/// admits is therefore never rejected again by this backstop.
///
/// # Errors
///
/// Returns [`PromqlError::Plan`] (HTTP 400 `bad_data`) when `step` is not
/// positive. Returns [`PromqlError::Plan`] (HTTP 400 `bad_data`) when the
/// interval count is more than [`MAX_RESOLUTION_POINTS`].
pub fn check_resolution_points(start_ms: i64, end_ms: i64, step: Time) -> Result<u64> {
    let step_ms = step.millis_i64();
    if step_ms <= 0 {
        return Err(PromqlError::Plan(format!(
            "zero or negative query resolution step widths are not accepted. Try a positive integer (step={step_ms}ms)"
        )));
    }
    // Reject on the interval count `(end - start) / step` (Prometheus' rule),
    // computed in u64 space so an abusive span can never overflow or wrap into a
    // small count.
    let span = u64::try_from(end_ms.saturating_sub(start_ms).max(0)).unwrap_or(u64::MAX);
    let step = u64::try_from(step_ms).unwrap_or(u64::MAX);
    let intervals = span / step;
    if intervals > MAX_RESOLUTION_POINTS {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .to_string(),
        ));
    }
    Ok(intervals.saturating_add(1))
}
