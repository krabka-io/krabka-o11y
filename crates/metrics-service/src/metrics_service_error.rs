#[derive(Debug, thiserror::Error)]
pub enum MetricsServiceError {
    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("compaction manifest decode failed: {0}")]
    Manifest(String),
}

impl From<object_store::Error> for MetricsServiceError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}

impl From<krabka_metrics::CompactionIndexError> for MetricsServiceError {
    fn from(error: krabka_metrics::CompactionIndexError) -> Self {
        Self::Manifest(error.to_string())
    }
}
