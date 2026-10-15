use super::*;

pub(crate) struct BlockBuildOptions<'a> {
    pub(crate) object_key_prefix: &'a str,
    pub(crate) promoted_attrs: &'a [PromotedSpanAttr],
}


/// Runtime settings for the block-builder loop.
#[derive(Clone, Debug)]
pub struct BlockBuilderConfig {
    pub object_key_prefix: String,
    pub index_key: String,
    /// How long each WAL poll waits for records.
    pub window: Time,
    /// Backoff after an empty WAL poll to prevent a transport-error busy loop.
    pub empty_poll_backoff: Time,
    pub promoted_attrs: Vec<PromotedSpanAttr>,
    /// Flush the accumulated buffer once this many span records are buffered.
    pub flush_max_records: usize,
    /// Flush the accumulated buffer once the oldest buffered record reaches
    /// this age.
    pub flush_max_age: Time,
    pub index_snapshot_retain: IndexSnapshotRetain,
}


/// Default maximum age of the oldest buffered span record before a flush.
///
/// In a cold-only deployment, such as the demo, there is no querier live tier.
/// The block-builder is then the only path that makes spans queryable, so this
/// age also bounds how stale recent-trace search and trace-by-id can be.
///
/// The value stays short, at 10s, so freshness stays close to the per-poll
/// behaviour. The `flush_max_records` cap still batches bursty traffic into
/// larger blocks, which is the proliferation case. A deployment that attaches a
/// querier live tier can raise this age to batch more aggressively at no
/// freshness cost.
pub const DEFAULT_FLUSH_MAX_AGE: Time = secs(10);


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

