use super::{BlockStoreError, CompactionIndexError, HistogramCodecError};

/// Errors raised while writing compacted metric blocks.
#[derive(Debug, thiserror::Error)]
pub enum CompactionWriteError {
    #[error(transparent)]
    Encode(#[from] HistogramCodecError),

    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}
