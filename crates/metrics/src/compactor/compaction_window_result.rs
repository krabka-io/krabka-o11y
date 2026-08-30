use super::{CompactedBlockWrite, CompactionPartitionOffset};

/// Result of processing one partition's compaction window.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionWindowResult {
    pub writes: Vec<CompactedBlockWrite>,
    pub committed_offset: Option<CompactionPartitionOffset>,
}
