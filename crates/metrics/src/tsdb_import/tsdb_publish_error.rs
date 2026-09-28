use super::{BlockStoreError, CompactionIndexError, HistogramCodecError};

/// Errors that stop the publication of a decoded TSDB block.
///
/// An error returned before the commit point leaves no live object: the
/// import deletes the Parquet objects that it wrote. An error after the commit
/// point leaves a committed import that a retry of the same block completes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TsdbPublishError {
    #[error("block {ulid} already holds content {existing}, not {uploaded}")]
    Conflict {
        ulid: String,
        existing: String,
        uploaded: String,
    },

    #[error("import record {key} has version {version}, and this build reads up to {supported}")]
    UnsupportedRecordVersion {
        key: String,
        version: u32,
        supported: u32,
    },

    #[error("import record {key} is malformed: {reason}")]
    InvalidRecord { key: String, reason: String },

    #[error("invalid import target: {0}")]
    InvalidTarget(String),

    #[error(transparent)]
    Encode(#[from] HistogramCodecError),

    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),

    #[error(transparent)]
    Index(#[from] CompactionIndexError),

    #[error("object store operation on {key} failed: {source}")]
    ObjectStore {
        key: String,
        #[source]
        source: object_store::Error,
    },
}
