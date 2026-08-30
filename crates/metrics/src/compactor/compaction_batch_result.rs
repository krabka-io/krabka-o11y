use super::{CompactionWindowResult, CompactedBlockWrite, CompactionPartitionOffset};

/// Result of processing a polled compaction batch across assigned partitions.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionBatchResult {
    pub partition_results: Vec<CompactionWindowResult>,
    pub writes: Vec<CompactedBlockWrite>,
    pub committed_offsets: Vec<CompactionPartitionOffset>,
}
