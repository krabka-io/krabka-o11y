/// Default width of one index shard, in the index's own timestamp ticks.
///
/// A day of milliseconds, which is the unit the metrics path counts in. The
/// shared index has no unit of its own, so this cannot be right for every
/// caller; [`super::MAX_INDEX_SHARDS_PER_TENANT`] is what keeps it from being
/// catastrophic for the ones it is wrong for, and
/// [`crate::Index::save_with_shard_width`] is how a caller that knows its unit
/// says so.
pub const DEFAULT_INDEX_SHARD_WIDTH: i64 = 24 * 60 * 60 * 1_000;
