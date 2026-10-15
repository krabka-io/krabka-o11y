use super::*;
use object_store::path::Path;

/// Drops every block that has fallen outside its tenant's retention window,
/// and returns the blocks it dropped.
///
/// The index is the record of what is live, so a block leaves the index first
/// and object storage second. This does the first half only. The caller saves
/// the snapshot, and then deletes the returned objects with
/// [`delete_trace_blocks`](super::delete_trace_blocks). If the object went
/// first, a querier could resolve a key whose object is already gone.
///
/// A traces block counts its bounds in epoch nanoseconds, so `now` is a
/// [`UnixNano`] and the window is measured in that unit. A tenant whose window
/// is `Time::ZERO` keeps every block it has. See
/// [`RetentionWindows::block_retention`].
pub fn expire_trace_blocks(
    index: &mut TraceIndex,
    now: UnixNano,
    windows: &dyn RetentionWindows,
) -> Vec<ExpiredBlock> {
    let expired = plan_expired_blocks(
        &index.compaction_candidates(),
        now.0,
        BlockTimestampUnit::Nanos,
        windows,
    );
    // The plan is ordered by tenant, so one pass over it groups the keys the
    // index wants per tenant without a map.
    let mut start = 0;
    while start < expired.len() {
        let tenant = expired[start].tenant.as_str();
        let end = expired[start..]
            .iter()
            .position(|block| block.tenant != tenant)
            .map_or(expired.len(), |offset| start + offset);
        let keys: Vec<String> = expired[start..end]
            .iter()
            .map(|block| block.object_key.clone())
            .collect();
        // `remove_trace_blocks` records a pending removal per block, so the
        // next snapshot merge cannot resurrect what this pass dropped.
        index.remove_trace_blocks(tenant, &keys);
        start = end;
    }
    expired
}


/// Deletes the span block objects that the trace index does not name.
///
/// A block no index names is unreachable: no query can find it and no
/// compaction will ever read it. An interrupted flush, a compaction whose index
/// save lost the race, and a deletion pass that stopped halfway all leave one
/// behind. `grace` keeps the sweep off the blocks a writer has put and not yet
/// published; pass
/// [`DEFAULT_BLOCK_SWEEP_GRACE`](krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE).
///
/// # The swept prefix
///
/// The sweep reaches only
/// [`TRACE_BLOCK_OBJECT_PREFIX`](crate::blockbuilder::TRACE_BLOCK_OBJECT_PREFIX)
/// under `object_key_prefix`, never the whole store. Both writers of a span
/// block name their object under that one prefix:
/// [`object_key`](crate::blockbuilder::object_key) for the block builder and
/// [`planned_compacted_object_key`](super::planned_compacted_object_key) for
/// the compactor. Nothing else on the traces path writes to object storage, so
/// every object the sweep can see is a span block.
///
/// # Errors
/// Returns [`LifecycleError`] when listing the prefix or the index objects
/// fails. The sweep then deletes nothing, because deleting on a partial listing
/// would delete live blocks.
pub async fn sweep_orphaned_trace_blocks(
    store: &Arc<dyn ObjectStore>,
    object_key_prefix: &str,
    trace_index_key: &str,
    index: &TraceIndex,
    grace: Time,
    now: SystemTime,
) -> Result<OrphanSweepStats, LifecycleError> {
    let prefix = prefixed_object_key(object_key_prefix, TRACE_BLOCK_OBJECT_PREFIX);
    let mut live_keys: BTreeSet<String> = index
        .compaction_candidates()
        .into_iter()
        .map(|candidate| candidate.object_key)
        .collect();
    live_keys.extend(
        list_index_object_keys(store, trace_index_key, &Path::from(prefix.as_str())).await?,
    );
    reconcile_orphans(store, &prefix, &live_keys, grace, now).await
}

