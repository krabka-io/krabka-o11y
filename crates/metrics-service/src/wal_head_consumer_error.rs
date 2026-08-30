use super::WalHeadReplayError;

#[derive(Debug, thiserror::Error)]
pub enum WalHeadConsumerError {
    #[error("metrics WAL consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] WalHeadReplayError),

    #[error("metrics WAL consumer commit failed: {0}")]
    Commit(String),
}
