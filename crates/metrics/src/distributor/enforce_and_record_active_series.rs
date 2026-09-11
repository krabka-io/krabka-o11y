use super::{BTreeSet, DecodedSeries, DistributorState, Instant, LimitError, Limits, TenantId};

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
