use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalRecord {
    pub value: Vec<u8>,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp_ms: Option<i64>,
    pub headers: Vec<KafkaWalHeader>,
}
