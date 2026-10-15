use super::*;

/// Enforces the per-user active-series limit and records the new series under a
/// single lock acquisition. The lock covers the check AND the insert, which
/// closes the active-series TOCTOU. Two concurrent pushes can no longer both see
/// the same pre-insert count and overshoot `max_global_series_per_user`.
///
/// The count is of *active* series: a series the tenant stopped writing to more
/// than `active_series_idle_timeout` ago no longer counts, so a tenant that
/// churns series does not walk into the cap. The count is also per process; see
/// [`SeriesTracker`](super::SeriesTracker) for what that costs.
pub(crate) fn enforce_and_record_active_series(
    state: &DistributorState,
    limits: &Limits,
    tenant: &TenantId,
    series: &[DecodedSeries],
    now: Instant,
) -> Result<(), LimitError> {
    let mut guard = state.series_tracker.lock();
    let tracked =
        state
            .series_tracker
            .enter(&mut guard, tenant, limits.active_series_idle_timeout, now);

    if limits.max_global_series_per_user != 0 {
        let current = tracked.series.len();
        let would_add = series
            .iter()
            .map(|series| series.labels.fingerprint())
            .filter(|fingerprint| !tracked.series.contains_key(fingerprint))
            .collect::<BTreeSet<_>>()
            .len();

        state.ingest_enforcer.check_active_series(
            limits,
            tenant.as_str(),
            u64::try_from(would_add).unwrap_or(u64::MAX),
            u64::try_from(current).unwrap_or(u64::MAX),
        )?;
    }

    for series in series {
        tracked.touch(series.labels.fingerprint(), now);
    }
    Ok(())
}

pub(crate) fn enforce_creation_grace_period(
    limits: &Limits,
    series: &[DecodedSeries],
    now_ms: i64,
) -> Result<(), PushError> {
    let newest_allowed_ms = now_ms.saturating_add(limits.creation_grace_period.millis_i64());
    if let Some(timestamp_ms) = series
        .iter()
        .filter_map(sample_timestamp_bounds)
        .map(|bounds| bounds.max_ms)
        .max()
        .filter(|timestamp| *timestamp > newest_allowed_ms)
    {
        return Err(PushError::TooFarInFuture {
            timestamp_ms,
            newest_allowed_ms,
        });
    }
    Ok(())
}

/// Applies every per-tenant ingest gate to `series`, in the order the push path
/// needs them.
///
/// The tenant's limits are resolved once here and handed to every gate, so one
/// request cannot get two verdicts on the same limit.
///
/// Returns `false` when the HA tracker drops the request, which is an accepted
/// request that writes nothing.
pub(crate) async fn enforce_ingest_limits(
    state: &DistributorState,
    tenant: &TenantId,
    series: &mut [DecodedSeries],
    clock_uncertainty: Time,
) -> Result<bool, PushError> {
    let limits = state.limits_for_tenant(tenant);
    let now = state.clock.now();
    validate(series, limits)?;
    enforce_label_limits(limits, series)?;
    enforce_creation_grace_period(limits, series, state.clock.now_unix_ms())?;
    // Decide-and-commit the in-memory HA winner atomically so a racing replica
    // cannot also win the same (tenant, cluster); only the durable Kafka persist
    // is left async, after the in-memory winner is already fixed.
    match state
        .tracker
        .elect_now_with_timeout(tenant.as_str(), series, state.ha_failover_timeout)
    {
        HaElection::Accept => {}
        HaElection::Drop => return Ok(false),
        HaElection::Elect(record) | HaElection::Update(record) => {
            // The in-memory winner is already committed under the tracker lock;
            // only the durable Kafka persist remains and may proceed async.
            if let Some(sink) = &state.ha_election_sink {
                sink.persist_election(record.clone()).await?;
            }
        }
    }

    strip_replica_label(series);
    enforce_and_record_active_series(state, limits, tenant, series, now)?;
    enforce_ingestion_rate(state, limits, tenant, series)?;
    enforce_out_of_order_window(state, limits, tenant, series, clock_uncertainty, now)?;
    Ok(true)
}

pub(crate) fn enforce_ingestion_rate(
    state: &DistributorState,
    limits: &Limits,
    tenant: &TenantId,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    let sample_count = decoded_sample_count(series);
    if sample_count == 0 {
        return Ok(());
    }

    state
        .ingest_enforcer
        .check_sample_rate(
            limits,
            tenant.as_str(),
            u64::try_from(sample_count).unwrap_or(u64::MAX),
        )
        .map_err(PushError::from)
}

pub(crate) fn enforce_label_limits(
    limits: &Limits,
    series: &[DecodedSeries],
) -> Result<(), LimitError> {
    for series in series {
        IngestEnforcer::check_labels(limits, &series.labels)?;
    }
    Ok(())
}

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
