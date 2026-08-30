/// WAL codec errors.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal encode failed: {0}")]
    Encode(String),

    #[error("wal decode failed: {0}")]
    Decode(String),
}
