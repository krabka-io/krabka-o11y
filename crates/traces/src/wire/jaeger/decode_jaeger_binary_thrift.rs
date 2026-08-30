use super::{BinaryInput, Span, WireError, read_binary_batch, spans_from_batch};

/// Decode a Jaeger binary-Thrift HTTP `Batch` body into internal spans.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_jaeger_binary_thrift(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let mut input = BinaryInput::new(body);
    let batch = read_binary_batch(&mut input)?;
    Ok(spans_from_batch(&batch))
}
