use super::WalHeadReplayError;

#[derive(Debug, thiserror::Error)]
/// An error from the metric WAL consumer that maintains the recent-sample head.
pub enum WalHeadConsumerError {
    #[error("metrics WAL consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] WalHeadReplayError),

    #[error("metrics WAL consumer commit failed: {0}")]
    Commit(String),
}
