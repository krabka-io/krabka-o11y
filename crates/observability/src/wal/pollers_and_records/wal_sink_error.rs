use super::{Error, ProducerError};

#[derive(Debug, Error)]
pub enum WalSinkError {
    #[error("wal sink append failed")]
    Append,
    #[error("wal record serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("wal producer failed: {0}")]
    Producer(#[from] ProducerError),
    #[error("wal producer delivery channel closed")]
    DeliveryCanceled,
}
