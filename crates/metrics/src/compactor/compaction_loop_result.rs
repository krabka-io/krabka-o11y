use super::CompactionPartitionOffset;

/// Summary returned after a compactor loop exits.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactionLoopResult {
    pub polls: usize,
    pub polled_records: usize,
    pub compacted_records: usize,
    pub writes: usize,
    pub committed_offsets: Vec<CompactionPartitionOffset>,
}
