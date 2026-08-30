#[derive(Debug, thiserror::Error)]
pub enum RulerStateWalRecordError {
    #[error("ruler state record encode failed: {0}")]
    Encode(String),

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}
