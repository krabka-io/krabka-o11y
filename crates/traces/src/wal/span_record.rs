use super::{Serialize, Deserialize, Span, TracesError};

/// One span's WAL record: tenant plus the OTLP-derived internal span.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpanRecord {
    pub tenant: String,
    pub span: Span,
}

impl SpanRecord {
    /// Encode with `serde-wincode`, which matches Krabka's serde-derived wire
    /// records.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn encode(&self) -> Result<Vec<u8>, TracesError> {
        <serde_wincode::SerdeCompat<SpanRecord> as wincode::Serialize>::serialize(self)
            .map_err(|err| TracesError::Wal(err.to_string()))
    }

    /// Decode a WAL record from `serde-wincode` bytes.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, TracesError> {
        <serde_wincode::SerdeCompat<SpanRecord> as wincode::Deserialize>::deserialize(bytes)
            .map_err(|err| TracesError::Wal(err.to_string()))
    }
}
