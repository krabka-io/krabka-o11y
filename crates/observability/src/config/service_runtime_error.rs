use super::*;

#[derive(Debug, Error)]
pub enum ServiceRuntimeError {
    #[error(transparent)]
    Config(#[from] ServiceConfigError),
    #[error(transparent)]
    Admin(#[from] AdminError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Producer(#[from] ProducerError),
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error(transparent)]
    Compactor(#[from] CompactorRunError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error("critical background task `{0}` stopped unexpectedly")]
    CriticalTask(&'static str),
}
