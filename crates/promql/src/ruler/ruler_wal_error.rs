
/// Errors that occur when the ruler writes output to the metrics WAL.
#[derive(Debug, thiserror::Error)]
pub enum RulerWalError {
    #[error("ruler wal append failed: {0}")]
    Append(String),
}
