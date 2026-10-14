#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeCheckpointEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
