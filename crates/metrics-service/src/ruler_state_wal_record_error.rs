#[derive(Debug, thiserror::Error)]
/// An error while encoding or decoding a ruler-state WAL record.
pub enum RulerStateWalRecordError {
    #[error("ruler state record encode failed: {0}")]
    Encode(String),

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}
