use super::{Offset, PartitionIndex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset: the next offset after the last replayed record.
    pub offset: Offset,
}
