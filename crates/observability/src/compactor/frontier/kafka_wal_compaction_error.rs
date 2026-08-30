use super::*;

#[derive(Debug, Error)]
pub enum KafkaWalCompactionError {
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
}
