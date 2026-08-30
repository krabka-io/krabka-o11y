use super::HaElectionReplayError;

#[derive(Debug, thiserror::Error)]
pub enum HaElectionConsumerError {
    #[error("HA election consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] HaElectionReplayError),

    #[error("HA election consumer commit failed: {0}")]
    Commit(String),
}
