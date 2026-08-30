use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset. This is the next offset after the last replayed
    /// record.
    pub offset: Offset,
}
