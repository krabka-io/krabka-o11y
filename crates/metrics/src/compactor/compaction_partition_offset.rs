use super::{PartitionIndex, Offset};

/// Offset to commit for one compacted WAL partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset. This is the next offset after the last durable
    /// record.
    pub offset: Offset,
}
