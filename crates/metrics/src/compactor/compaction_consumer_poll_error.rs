/// Errors raised by a consumer poll.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerPollError {
    #[error("consumer poll failed: {0}")]
    Poll(String),
    #[error("WAL drain failed: {0}")]
    Drain(#[source] krabka_client_consumer::ConsumerError),
}
