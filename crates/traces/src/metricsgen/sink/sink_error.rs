
/// Errors that cross the metrics-generator source and sink boundaries.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("source error: {0}")]
    Source(String),
}
