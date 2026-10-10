use super::{BTreeMap, IndexShardRange, PendingRemoval, shard_ranges_for_span};

/// The slots a merge has to read: every one the span of a contributed block
/// or of a removal reaches into.
///
/// Every shard that meets one of these has to be read, because the record it
/// replaces or retires can only be in one of them.
pub(crate) fn touched_shard_ranges(
    contributed: impl IntoIterator<Item = IndexShardRange>,
    removed: Option<&BTreeMap<String, PendingRemoval>>,
    width: i64,
) -> Vec<IndexShardRange> {
    let removed = removed
        .into_iter()
        .flat_map(BTreeMap::values)
        .map(|removal| IndexShardRange::new(removal.min_ts, removal.max_ts));
    contributed
        .into_iter()
        .chain(removed)
        .flat_map(|span| shard_ranges_for_span(span.start, span.end, width))
        .collect()
}
