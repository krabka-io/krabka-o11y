use super::{
    CompactorRunError, Instant, ObjectPath, ObjectStore, OverridesProvider, SystemTime,
    TenantCompactionIndexCache, Time, TimeExt, UNIX_EPOCH, sweep_expired_log_blocks,
};

/// Runs the retention sweep when one is due, and drops the index cache after
/// it.
///
/// The sweep runs here rather than in a task of its own. Every log index write
/// is a whole-object put with last-writer-wins semantics, and this loop holds
/// the tenant indexes in memory between batches. A sweeper in another process
/// would rewrite a manifest from a snapshot taken before this loop's last put,
/// and the block that put had just published would be gone from the index.
///
/// A deployment where no tenant has a window pays one call to
/// [`OverridesProvider::expires_any_blocks`] per loop and nothing else. The
/// interval bounds what the rest pay: a sweep reads every index of every
/// tenant, and this loop turns over far faster than a retention window moves.
///
/// # Errors
/// Returns the block-store error when the store cannot be listed, or when an
/// index cannot be read or written.
pub(crate) async fn sweep_log_retention_before_compaction(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    overrides: &OverridesProvider,
    tenant_indexes: &mut TenantCompactionIndexCache,
    next_sweep: &mut Instant,
    sweep_interval: Time,
) -> Result<(), CompactorRunError> {
    if !overrides.expires_any_blocks() {
        return Ok(());
    }
    let started = Instant::now();
    if started < *next_sweep {
        return Ok(());
    }
    *next_sweep = started + sweep_interval.to_std();

    // A clock before the epoch, or past the nanosecond range of an `i64`,
    // expires nothing rather than everything.
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .unwrap_or(i64::MIN);
    sweep_expired_log_blocks(store, prefix, now_ns, overrides).await?;
    // The sweep rewrote manifests that the cache may hold, so the cache goes
    // with them.
    tenant_indexes.clear();
    Ok(())
}
