use super::{DistributorState, Limits, DecodedSeries, LimitError, BTreeSet};

/// Enforces the per-user active-series limit and records the new series under a
/// single lock acquisition. The lock covers the check AND the insert, which
/// closes the active-series TOCTOU. Two concurrent pushes can no longer both see
/// the same pre-insert count and overshoot `max_global_series_per_user`.
pub(crate) fn enforce_and_record_active_series(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), LimitError> {
    let mut active = state
        .active_series
        .lock()
        .expect("active series tracker poisoned");

    if limits.max_global_series_per_user != 0 {
        let existing = active.get(tenant);
        let current = existing.map_or(0, BTreeSet::len);
        let would_add = series
            .iter()
            .map(|series| series.labels.fingerprint())
            .filter(|fingerprint| existing.is_none_or(|set| !set.contains(fingerprint)))
            .collect::<BTreeSet<_>>()
            .len();

        state.ingest_enforcer.check_active_series(
            limits,
            tenant,
            u64::try_from(would_add).unwrap_or(u64::MAX),
            u64::try_from(current).unwrap_or(u64::MAX),
        )?;
    }

    let tenant_active = active.entry(tenant.to_string()).or_default();
    for series in series {
        tenant_active.insert(series.labels.fingerprint());
    }
    Ok(())
}
