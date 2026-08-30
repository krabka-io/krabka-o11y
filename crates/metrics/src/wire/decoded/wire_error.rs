/// Errors raised at the `remote_write` ingest edge.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unsupported content type `{0}`")]
    UnsupportedContentType(String),
    #[error("unsupported content encoding `{0}`")]
    UnsupportedContentEncoding(String),
    #[error("snappy decoded body exceeds max_output={0}")]
    SnappyOutputTooLarge(usize),
    #[error("snappy decode failed: {0}")]
    SnappyDecode(String),
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(String),
    #[error("invalid remote_write request: {0}")]
    Invalid(String),
}

impl WireError {
    /// HTTP status code for Prometheus `remote_write` ingest.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedContentType(_) | Self::UnsupportedContentEncoding(_) => 415,
            Self::SnappyOutputTooLarge(_)
            | Self::SnappyDecode(_)
            | Self::ProtobufDecode(_)
            | Self::Invalid(_) => 400,
        }
    }
}
