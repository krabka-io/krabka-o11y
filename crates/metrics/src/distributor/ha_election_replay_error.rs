use super::{Offset, PartitionIndex};

#[derive(Debug, thiserror::Error)]
pub enum HaElectionReplayError {
    #[error("HA election record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("HA election record decode failed: {0}")]
    Decode(String),
}
