use super::*;

/// Tests the per-tenant max-series limit and reserves the new fingerprints.
///
/// The function holds the `active_series` lock across both the test and the
/// insertion, so the operation is atomic. Under two separate lock holds, two
/// concurrent requests can each pass the test and together exceed
/// `max_series`.
///
/// On success the function returns the subset of the fingerprints of `records`
/// that this call inserted, that is, the ones that were not already present.
/// The caller must pass this set to [`rollback_reserved_series`] if the WAL
/// append after it fails. A rejected or failed write then never permanently
/// inflates the tenant's series count. When `max_series` is unlimited (`0`),
/// the function makes no reservation and returns an empty set. It does not
/// track cardinality in that case.
pub(crate) fn enforce_and_reserve_max_series(
    state: &DistributorState,
    tenant: &str,
    records: &[ProfileRecord],
) -> Result<Vec<u64>, ProfilesError> {
    let limit = state.profile_overrides.for_tenant(tenant).max_series;
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut active = state
        .active_series
        .lock()
        .map_err(|_| ProfilesError::Internal("active series lock poisoned".to_string()))?;

    // Bound per-tenant map growth: evict an arbitrary existing tenant before
    // admitting a brand-new one once the cap is hit.
    if !active.contains_key(tenant) && active.len() >= state.max_tracked_tenants {
        evict_one_tenant(&mut active);
    }
    let entry = active.entry(tenant.to_string()).or_default();

    // Compute the DISTINCT fingerprints this request would newly add, without
    // mutating `entry` yet, so a rejection leaves the set untouched (no partial
    // writes on limit failure). Deduping here means a request that repeats the
    // same new fingerprint counts it once.
    let mut to_add: BTreeSet<u64> = BTreeSet::new();
    for rec in records {
        let fingerprint = rec.series_fingerprint();
        if !entry.contains(&fingerprint) {
            to_add.insert(fingerprint);
        }
    }
    let projected = entry.len() + to_add.len();
    if u64::try_from(projected).unwrap_or(u64::MAX) > limit {
        return Err(crate::limits::LimitError::MaxSeries {
            limit,
            observed: u64::try_from(projected).unwrap_or(u64::MAX),
        }
        .into());
    }

    // Within budget: reserve the new fingerprints and report exactly which ones
    // were inserted so the caller can roll them back on a later failure.
    for fingerprint in &to_add {
        entry.insert(*fingerprint);
    }
    Ok(to_add.into_iter().collect())
}
