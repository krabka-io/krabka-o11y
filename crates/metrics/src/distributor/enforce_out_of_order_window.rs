use super::{
    DecodedSeries, DistributorState, Instant, Limits, PushError, TenantId, Time, TimeExt,
    sample_timestamp_bounds,
};

/// Rejects samples older than the tenant's out-of-order window, measured
/// against the newest sample the tracker holds for the same series.
///
/// The window is enforced per process, against this replica's own view of the
/// series; see [`SeriesTracker`](super::SeriesTracker).
pub(crate) fn enforce_out_of_order_window(
    state: &DistributorState,
    limits: &Limits,
    tenant: &TenantId,
    series: &[DecodedSeries],
    now: Instant,
) -> Result<(), PushError> {
    if limits.out_of_order_time_window < Time::ZERO {
        return Ok(());
    }
    let window_ms = limits.out_of_order_time_window.millis_i64();

    let mut guard = state.series_tracker.lock();
    let tracked =
        state
            .series_tracker
            .enter(&mut guard, tenant, limits.active_series_idle_timeout, now);
    let mut updates = Vec::new();
    for series in series {
        let Some((min_timestamp, max_timestamp)) = sample_timestamp_bounds(series) else {
            continue;
        };
        let fingerprint = series.labels.fingerprint();
        if let Some(previous_latest) = tracked
            .series
            .get(&fingerprint)
            .and_then(|activity| activity.latest_sample_ms)
        {
            let oldest_allowed = previous_latest - window_ms;
            if min_timestamp < oldest_allowed {
                return Err(PushError::TooOldSample {
                    timestamp_ms: min_timestamp,
                    oldest_allowed_ms: oldest_allowed,
                });
            }
        }
        updates.push((fingerprint, max_timestamp));
    }

    for (fingerprint, max_timestamp) in updates {
        let activity = tracked.touch(fingerprint, now);
        activity.latest_sample_ms = Some(
            activity
                .latest_sample_ms
                .map_or(max_timestamp, |previous| previous.max(max_timestamp)),
        );
    }
    Ok(())
}
