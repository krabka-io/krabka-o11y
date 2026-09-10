use super::{WireError, span_id_u64_from_be_slice};

// Jaeger's span id is the signed reading of the same eight big-endian bytes
// OTLP carries. Eight bytes exactly: a short or long id is a decode error,
// never a pad or a truncation.
pub(crate) fn span_id_part(bytes: &[u8]) -> Result<i64, WireError> {
    span_id_u64_from_be_slice(bytes)
        .map(u64::cast_signed)
        .ok_or_else(|| WireError::Decode("jaeger span_id must be 8 bytes".into()))
}
