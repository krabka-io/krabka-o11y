use super::{ConsumerError, Error};

#[derive(Debug, Error)]
pub enum WalConsumerError {
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error("WAL consumer record {topic}-{partition}@{offset} did not include a value")]
    MissingValue {
        topic: String,
        partition: i32,
        offset: i64,
    },
}
