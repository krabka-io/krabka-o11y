use super::IndexShardRange;

/// The shard that holds what the grid cannot place.
///
/// Two kinds of thing land here: a record whose span crosses more slots than
/// [`super::MAX_SHARD_SLOTS_PER_RECORD`] allows, and a series no block carries
/// yet, which has no span at all. The range meets every query and every
/// touched range, so this shard is always listed, always read and always
/// fetched by a merge. It is meant to be empty.
pub(crate) const UNBOUNDED_SHARD_RANGE: IndexShardRange = IndexShardRange::new(i64::MIN, i64::MAX);
