use super::{ DecodedSeries, DistributorState, Limits, PushError, Time, TimeExt, sample_timestamp_bounds};

pub(crate) fn enforce_out_of_order_window(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    if limits.out_of_order_time_window < Time::ZERO {
        return Ok(());
    }
    let window_ms = limits.out_of_order_time_window.millis_i64();

    let mut latest = state
        .latest_timestamps
        .lock()
        .expect("latest timestamp tracker poisoned");
    let mut updates = Vec::new();
    for series in series {
        let Some((min_timestamp, max_timestamp)) = sample_timestamp_bounds(series) else {
            continue;
        };
        let fingerprint = series.labels.fingerprint();
        let key = (tenant.to_string(), fingerprint);
        if let Some(previous_latest) = latest.get(&key).copied() {
            let oldest_allowed = previous_latest - window_ms;
            if min_timestamp < oldest_allowed {
                return Err(PushError::TooOldSample {
                    timestamp_ms: min_timestamp,
                    oldest_allowed_ms: oldest_allowed,
                });
            }
        }
        updates.push((key, max_timestamp));
    }

    for (key, max_timestamp) in updates {
        latest
            .entry(key)
            .and_modify(|previous| *previous = (*previous).max(max_timestamp))
            .or_insert(max_timestamp);
    }
    Ok(())
}
