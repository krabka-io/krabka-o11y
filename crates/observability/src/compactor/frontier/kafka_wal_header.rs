#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
