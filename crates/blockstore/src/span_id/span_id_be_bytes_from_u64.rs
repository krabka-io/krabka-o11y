/// Writes a `u64` span id back as its eight big-endian bytes.
///
/// The inverse of [`span_id_u64_from_be_bytes`](super::span_id_u64_from_be_bytes).
#[must_use]
pub const fn span_id_be_bytes_from_u64(span_id: u64) -> [u8; 8] {
    span_id.to_be_bytes()
}
