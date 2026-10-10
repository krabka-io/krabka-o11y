use super::{Arc, ByteSize, IndexShardRange, IndexSnapshotRetain, ObjectStore};

/// A publish of one index writer's contribution as the next snapshot
/// generation under `key`, as `save_latest_snapshot_with_retain_and_max_bytes`
/// on [`TraceIndex`](crate::TraceIndex) and
/// [`ProfileIndex`](crate::ProfileIndex) takes it.
#[derive(Clone, Copy)]
pub struct IndexSnapshotPublish<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    /// The snapshot key the generation is published under.
    pub key: &'a str,
    /// How many generations to keep after this one lands.
    pub retain: IndexSnapshotRetain,
    /// The size cap of each manifest and shard payload read or written.
    pub max_bytes: ByteSize,
}

/// A load of the latest published snapshot generation under `key`, kept to
/// one tenant's shards that meet an inclusive time span, as
/// `load_latest_snapshot_for_range_with_max_bytes` on
/// [`TraceIndex`](crate::TraceIndex) and
/// [`ProfileIndex`](crate::ProfileIndex) takes it.
#[derive(Clone, Copy)]
pub struct TenantSnapshotRangeRead<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    /// The snapshot key to read the latest generation of.
    pub key: &'a str,
    /// The one tenant whose shards are read.
    pub tenant: &'a str,
    /// The inclusive span, in the index's own timestamp unit (nanoseconds
    /// for traces, milliseconds for profiles), a read shard must meet.
    pub span: IndexShardRange,
    /// The size cap of each manifest and shard payload read.
    pub max_bytes: ByteSize,
}
