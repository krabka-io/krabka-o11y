use super::{
    ArrowError, BlockStoreError, CompactionIndexError, CompactionManifestError, HistogramCodecError,
};

/// Errors raised while merging metric blocks that are already in object storage.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetricCompactionError {
    #[error(transparent)]
    Manifest(#[from] CompactionManifestError),

    #[error(transparent)]
    Index(#[from] CompactionIndexError),

    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),

    #[error(transparent)]
    Codec(#[from] HistogramCodecError),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// A job named an input the index does not hold.
    ///
    /// The planner reads the same manifests the merge does, so this is a fault
    /// in the compactor rather than in the bucket.
    #[error("compaction input `{object_key}` has no index manifest")]
    UnknownInput { object_key: String },

    /// Two inputs of one job do not carry the same Arrow schema.
    ///
    /// A merge writes one block, so its inputs must agree on the columns. Each
    /// metric block kind has exactly one schema, so two that disagree were not
    /// written by the same code.
    #[error("compaction inputs disagree on their schema: `{first}` and `{second}`")]
    SchemaMismatch { first: String, second: String },
}
