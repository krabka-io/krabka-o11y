use super::{PartitionIndex, WalError, CompactionWriteError, CompactionCommitError};

/// Errors raised while processing a compaction WAL window.
#[derive(Debug, thiserror::Error)]
pub enum CompactionWindowError {
    #[error("compaction window spans multiple partitions: {first} and {second}")]
    MultiplePartitions {
        first: PartitionIndex,
        second: PartitionIndex,
    },

    #[error(transparent)]
    Wal(#[from] WalError),

    #[error(transparent)]
    Write(#[from] CompactionWriteError),

    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}
