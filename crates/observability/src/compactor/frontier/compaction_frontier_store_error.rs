use super::Error;

#[derive(Debug, Error)]
pub enum CompactionFrontierStoreError {
    #[error("invalid compaction frontier manifest version {actual}; expected {expected}")]
    InvalidVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}
