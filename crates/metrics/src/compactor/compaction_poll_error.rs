use super::*;

/// Errors raised by one compactor poll/process pass.
#[derive(Debug, thiserror::Error)]
pub enum CompactionPollError {
    #[error(transparent)]
    Poll(#[from] CompactionConsumerPollError),

    #[error(transparent)]
    ConsumerRecord(#[from] CompactionConsumerRecordError),

    #[error(transparent)]
    Window(#[from] CompactionWindowError),
}
