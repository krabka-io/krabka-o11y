use super::{CompactionManifestError, CompactionRetentionStats, LifecycleError};

/// Errors raised while deleting compacted metric objects outside retention.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionRetentionError {
    #[error(transparent)]
    Manifest(#[from] CompactionManifestError),

    #[error("{source}")]
    Lifecycle {
        #[source]
        source: LifecycleError,
        stats: Box<CompactionRetentionStats>,
    },
}

impl CompactionRetentionError {
    /// Work completed before a fallible orphan listing failed.
    #[must_use]
    pub fn partial_stats(&self) -> Option<&CompactionRetentionStats> {
        match self {
            Self::Lifecycle { stats, .. } => Some(stats),
            Self::Manifest(_) => None,
        }
    }
}
