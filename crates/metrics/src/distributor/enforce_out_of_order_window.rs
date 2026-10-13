use super::{
    DecodedSeries, DistributorState, Instant, Limits, PushError, TenantId, Time, TimeExt,
    sample_timestamp_bounds,
};

struct SeriesTimestampUpdate {
    fingerprint: u64,
    max_timestamp: i64,
}

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
    clock_uncertainty: Time,
    now: Instant,
) -> Result<(), PushError> {
    if limits.out_of_order_time_window < Time::ZERO {
        return Ok(());
    }
    let mut guard = state.series_tracker.lock();
    let tracked =
        state
            .series_tracker
            .enter(&mut guard, tenant, limits.active_series_idle_timeout, now);
    let window_ms = limits
        .out_of_order_time_window
        .max(tracked.clock_uncertainty)
        .max(clock_uncertainty)
        .millis_i64();
    let mut updates = Vec::new();
    for series in series {
        let Some(bounds) = sample_timestamp_bounds(series) else {
            continue;
        };
        let min_timestamp = bounds.min_ms;
        let max_timestamp = bounds.max_ms;
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
        updates.push(SeriesTimestampUpdate {
            fingerprint,
            max_timestamp,
        });
    }

    for update in updates {
        let activity = tracked.touch(update.fingerprint, now);
        activity.latest_sample_ms = Some(
            activity
                .latest_sample_ms
                .map_or(update.max_timestamp, |previous| {
                    previous.max(update.max_timestamp)
                }),
        );
    }
    Ok(())
}
