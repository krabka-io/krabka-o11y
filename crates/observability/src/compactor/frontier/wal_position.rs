use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalPosition {
    pub partition: PartitionIndex,
    pub offset: Offset,
}
