use super::{
    CompactionPolicy, DownsamplePolicy, IndexSnapshotRetain, RetentionWindows, SystemTime, Time,
};

/// What one pass of [`run_lifecycle_pass`](super::run_lifecycle_pass) is
/// allowed to do.
pub struct LifecycleOptions<'a> {
    /// The index snapshot chain the pass loads from and publishes to.
    pub index_key: &'a str,
    /// How many earlier snapshot generations the save keeps.
    pub index_snapshot_retain: IndexSnapshotRetain,
    /// Which merges the pass plans.
    pub policy: CompactionPolicy,
    /// The coarser time buckets a merge sums its samples into, if any.
    pub downsample: Option<DownsamplePolicy>,
    /// How long each tenant's blocks are kept.
    pub retention: &'a dyn RetentionWindows,
    /// The object-store prefix the orphan sweep lists. Every object under it
    /// that the index does not name is deleted, so it must name a location the
    /// profile blocks own. Pass
    /// [`BLOCK_OBJECT_PREFIX`](crate::blockbuilder::BLOCK_OBJECT_PREFIX).
    pub block_prefix: &'a str,
    /// How long an object the index does not name is left alone before the
    /// orphan sweep may delete it.
    pub orphan_grace: Time,
    /// The clock the retention cutoff and the grace window are measured from.
    pub now: SystemTime,
}
