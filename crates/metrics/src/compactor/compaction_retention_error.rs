use super::{CompactionManifestError, LifecycleError};

/// Errors raised while deleting compacted metric objects outside retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionRetentionError {
    #[error(transparent)]
    Manifest(#[from] CompactionManifestError),

    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
}
