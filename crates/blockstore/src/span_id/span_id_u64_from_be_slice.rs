use super::span_id_u64_from_be_bytes;

/// Reads a span id off the wire as a big-endian `u64`.
///
/// Returns `None` unless the slice is exactly eight bytes long, so a short or
/// long id is a decode failure for the caller to report, never a pad or a
/// truncation.
#[must_use]
pub fn span_id_u64_from_be_slice(bytes: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(bytes)
        .ok()
        .map(span_id_u64_from_be_bytes)
}
