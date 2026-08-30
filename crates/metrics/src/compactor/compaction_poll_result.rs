use super::CompactionBatchResult;

/// Result of one compactor consumer poll and processing pass.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionPollResult {
    pub polled_records: usize,
    pub compacted_records: usize,
    pub batch: CompactionBatchResult,
}
