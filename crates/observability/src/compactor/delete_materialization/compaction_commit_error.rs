use super::*;

#[derive(Debug, Error)]
#[error("offset commit failed")]
pub struct CompactionCommitError;
