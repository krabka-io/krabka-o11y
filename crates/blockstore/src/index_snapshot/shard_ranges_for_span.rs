use super::{
    IndexShardRange, MAX_SHARD_SLOTS_PER_RECORD, UNBOUNDED_SHARD_RANGE, shard_range_of_slot,
};

/// The shards a record spanning `[min_ts, max_ts]` belongs to.
///
/// A record belongs to every slot of the grid its own span crosses, so a
/// record that straddles a boundary is written into both, and a query that
/// reads two shards can see it twice. Callers merge records by object key, so
/// the duplicate costs nothing but the bytes. Cutting shards on record
/// boundaries instead is what would make an append rewrite its neighbours,
/// which is the cost this whole layout exists to avoid.
///
/// The grid width is fixed rather than derived from the tenant's span, which
/// the shared [`crate::Index`] does derive. Deriving it needs the tenant's
/// whole span, and reading the whole tenant to publish one shard is the thing
/// a manifest is here to stop.
pub(crate) fn shard_ranges_for_span(min_ts: i64, max_ts: i64, width: i64) -> Vec<IndexShardRange> {
    let width = width.max(1);
    let (min_ts, max_ts) = if min_ts <= max_ts {
        (min_ts, max_ts)
    } else {
        (max_ts, min_ts)
    };
    let first = min_ts.div_euclid(width);
    let last = max_ts.div_euclid(width);
    if last.saturating_sub(first) >= MAX_SHARD_SLOTS_PER_RECORD {
        return vec![UNBOUNDED_SHARD_RANGE];
    }
    (first..=last)
        .map(|slot| shard_range_of_slot(slot, width))
        .collect()
}
