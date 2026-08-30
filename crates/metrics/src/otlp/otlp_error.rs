use super::*;

/// Errors raised by OTLP metrics translation.
#[derive(Debug, thiserror::Error)]
pub enum OtlpError {
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
    #[error("delta temporality is not supported yet for metric `{0}`")]
    DeltaUnsupported(String),
    #[error("invalid OTLP metric `{0}`: {1}")]
    Invalid(String, String),
    #[error("unsupported OTLP metric `{0}`: {1}")]
    Unsupported(String, String),
}

impl OtlpError {
    /// HTTP status code for OTLP HTTP ingest.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        400
    }
}
