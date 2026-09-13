use super::{Offset, PartitionIndex};

#[derive(Debug, thiserror::Error)]
/// An error while applying one ruler-state WAL record.
pub enum RulerStateReplayError {
    #[error("ruler state record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}
