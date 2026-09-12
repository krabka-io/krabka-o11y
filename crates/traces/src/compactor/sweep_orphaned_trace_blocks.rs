use super::{
    Arc, BTreeSet, BlockSweepError, ObjectStore, OrphanSweepStats, Path, SystemTime,
    TRACE_BLOCK_OBJECT_PREFIX, Time, TraceIndex, prefixed_object_key, reconcile_orphans,
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
/// The one exception is the trace index, whose location an operator chooses
/// with `--trace-index-key`. `trace_index_key` is that key, and the sweep
/// refuses to run when it names a location inside the swept prefix. See
/// [`BlockSweepError::IndexInsideBlockPrefix`].
///
/// # Errors
/// Returns [`BlockSweepError::IndexInsideBlockPrefix`] when the index is inside
/// the prefix this would sweep, and [`BlockSweepError::Lifecycle`] when listing
/// the prefix fails. The sweep then deletes nothing, because deleting on a
/// partial listing would delete live blocks.
pub async fn sweep_orphaned_trace_blocks(
    store: &Arc<dyn ObjectStore>,
    object_key_prefix: &str,
    trace_index_key: &str,
    index: &TraceIndex,
    grace: Time,
    now: SystemTime,
) -> Result<OrphanSweepStats, BlockSweepError> {
    let prefix = prefixed_object_key(object_key_prefix, TRACE_BLOCK_OBJECT_PREFIX);
    // The same path-prefix test the listing itself uses, so what this rejects
    // is exactly what the sweep would have reached. The index's snapshots,
    // shard manifests and shard payloads are siblings of the key, so they share
    // its parent and are inside the prefix if and only if the key is.
    if Path::from(trace_index_key).prefix_matches(&Path::from(prefix.as_str())) {
        return Err(BlockSweepError::IndexInsideBlockPrefix {
            prefix,
            trace_index_key: trace_index_key.to_string(),
        });
    }

    let live_keys: BTreeSet<String> = index
        .compaction_candidates()
        .into_iter()
        .map(|candidate| candidate.object_key)
        .collect();
    Ok(reconcile_orphans(store, &prefix, &live_keys, grace, now).await?)
}
