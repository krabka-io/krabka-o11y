use krabka_o11y_verified::shard_range;

use super::IndexShardRange;

/// The span of grid slot `slot`, clamped where the grid runs off the ends of
/// an `i64`.
pub(crate) fn shard_range_of_slot(slot: i64, width: i64) -> IndexShardRange {
    let (start, end) = shard_range(slot, width);
    IndexShardRange::new(start, end)
}
