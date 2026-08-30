
/// Errors raised while appending to the metrics WAL.
#[derive(Debug, thiserror::Error)]
pub enum ProduceError {
    #[error("wal append failed: {0}")]
    Append(String),
}
