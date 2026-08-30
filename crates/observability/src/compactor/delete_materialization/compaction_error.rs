use super::{BlockStoreError, CompactionCommitError, Error};

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("cannot compact an empty WAL batch")]
    EmptyWalBatch,
    #[error("cannot compact WAL batch after all rows were deleted")]
    AllRowsDeleted,
    #[error("missing WAL position for record at timestamp {timestamp_ns}")]
    MissingWalPosition { timestamp_ns: i64 },
    #[error("cannot compact mixed-tenant WAL batch: expected {expected}, got {actual}")]
    MixedTenant { expected: String, actual: String },
    #[error("cannot compact mixed-partition WAL batch: expected {expected}, got {actual}")]
    MixedPartition { expected: i32, actual: i32 },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}
