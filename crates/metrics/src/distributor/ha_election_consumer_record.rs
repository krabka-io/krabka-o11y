use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}
