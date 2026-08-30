
/// Counts of stale compacted objects removed by a retention sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionRetentionStats {
    pub manifests_scanned: usize,
    pub manifests_deleted: usize,
    pub blocks_deleted: usize,
}
