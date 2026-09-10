use super::IndexShardRange;

/// The span of grid slot `slot`, clamped where the grid runs off the ends of
/// an `i64`.
pub(crate) fn shard_range_of_slot(slot: i64, width: i64) -> IndexShardRange {
    let start = slot.checked_mul(width).unwrap_or(i64::MIN);
    let end = start.saturating_add(width.saturating_sub(1));
    IndexShardRange::new(start, end)
}
