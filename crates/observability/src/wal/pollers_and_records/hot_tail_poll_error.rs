use super::{Error, WalConsumerError, WalRecordDecodeError};

#[derive(Debug, Error)]
pub enum HotTailPollError {
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
}
