use super::{Offset, PartitionIndex};

#[derive(Debug, thiserror::Error)]
/// An error while applying one metric WAL record to the recent-sample head.
pub enum WalHeadReplayError {
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("metrics WAL record decode failed: {0}")]
    Decode(String),
}
