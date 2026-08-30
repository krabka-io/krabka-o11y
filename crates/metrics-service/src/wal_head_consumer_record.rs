use super::{Offset, PartitionIndex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}
