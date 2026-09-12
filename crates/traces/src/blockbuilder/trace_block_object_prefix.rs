/// The one object-store prefix every traces block is written under.
///
/// Both writers put their blocks here: the block builder through
/// [`object_key`](super::object_key), and the compactor through
/// [`planned_compacted_object_key`](crate::compactor::planned_compacted_object_key).
/// Nothing else of the traces path is written under it, so it is also the
/// prefix the orphan sweep is safe to reconcile. The trace index and its
/// snapshots and shards live under the operator's `--trace-index-key`, which
/// is a separate location.
///
/// An object key a caller hands to the store is this prefix under the
/// deployment's own object-key prefix. See
/// [`prefixed_object_key`](super::prefixed_object_key).
pub const TRACE_BLOCK_OBJECT_PREFIX: &str = "traces";
