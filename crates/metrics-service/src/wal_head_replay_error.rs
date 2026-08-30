use super::{Offset, PartitionIndex};

#[derive(Debug, thiserror::Error)]
pub enum WalHeadReplayError {
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("metrics WAL record decode failed: {0}")]
    Decode(String),
}
