use super::{Offset, PartitionIndex};

/// One encoded WAL record fetched by the compactor for a single topic partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionWalRecord {
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Vec<u8>,
}
