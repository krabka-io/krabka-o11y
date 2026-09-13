use super::RulerStateReplayError;

#[derive(Debug, thiserror::Error)]
/// An error from the consumer that restores ruler groups and active alerts.
pub enum RulerStateConsumerError {
    #[error("ruler state consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] RulerStateReplayError),

    #[error("ruler state consumer commit failed: {0}")]
    Commit(String),
}
