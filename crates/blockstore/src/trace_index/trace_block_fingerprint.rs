use super::{DefaultHasher, Hash as _, Hasher as _, TraceBlockStats};

/// Fingerprint of the trace-block record a writer holds.
///
/// A pending removal pins itself to this, so replaying the removal against a
/// merge base cannot drop a *different* block that has since been written under
/// the same object key. Records are only ever compared inside one process, so
/// the hasher does not have to be stable across builds or hosts; it only has to
/// be a function of everything the record says. The fields are named one by one
/// rather than hashing [`TraceBlockStats`] wholesale, so a field added there is
/// a deliberate choice here rather than a silent change of identity.
pub(crate) fn trace_block_fingerprint(stats: &TraceBlockStats) -> u64 {
    let mut hasher = DefaultHasher::new();
    stats.object_key.hash(&mut hasher);
    stats.min_ts.hash(&mut hasher);
    stats.max_ts.hash(&mut hasher);
    stats.tag_names.hash(&mut hasher);
    stats.tag_values.hash(&mut hasher);
    stats.row_count.hash(&mut hasher);
    stats.level.hash(&mut hasher);
    // `ShardedTraceBloom` is not `Hash`, and its shards are what say which
    // traces the block holds, so they are folded in by hand.
    for shard in &stats.bloom.shards {
        shard.bits.hash(&mut hasher);
        shard.num_bits.hash(&mut hasher);
        shard.k.hash(&mut hasher);
    }
    hasher.finish()
}
