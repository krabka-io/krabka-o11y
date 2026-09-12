use super::{
    BlockTimestampUnit, ExpiredBlock, RetentionWindows, TraceIndex, UnixNano, plan_expired_blocks,
};

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
