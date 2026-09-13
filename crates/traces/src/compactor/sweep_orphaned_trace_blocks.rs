use object_store::path::Path;

use super::{
    Arc, BTreeSet, LifecycleError, ObjectStore, OrphanSweepStats, SystemTime,
    TRACE_BLOCK_OBJECT_PREFIX, Time, TraceIndex, list_index_object_keys, prefixed_object_key,
    reconcile_orphans,
};

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
