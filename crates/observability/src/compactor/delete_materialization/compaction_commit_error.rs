use super::Error;

#[derive(Debug, Error)]
#[error("offset commit failed")]
pub struct CompactionCommitError;
