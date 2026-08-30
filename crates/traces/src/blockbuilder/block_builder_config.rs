use super::{IndexSnapshotRetain, PromotedSpanAttr, Time};

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
