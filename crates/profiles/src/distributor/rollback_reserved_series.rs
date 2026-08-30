use super::*;

/// Undoes a max-series reservation from [`enforce_and_reserve_max_series`].
///
/// The function removes only the fingerprints that this request inserted, which
/// `reserved` tracks. Concurrent requests that legitimately share a series stay
/// unaffected. A poisoned lock here is best-effort. The function cannot recover
/// the set, so it logs the fault and continues instead of a panic.
pub(crate) fn rollback_reserved_series(state: &DistributorState, tenant: &str, reserved: &[u64]) {
    if reserved.is_empty() {
        return;
    }
    let Ok(mut active) = state.active_series.lock() else {
        tracing::error!(tenant, "active series lock poisoned during rollback");
        return;
    };
    if let Some(entry) = active.get_mut(tenant) {
        for fingerprint in reserved {
            entry.remove(fingerprint);
        }
        if entry.is_empty() {
            active.remove(tenant);
        }
    }
}
