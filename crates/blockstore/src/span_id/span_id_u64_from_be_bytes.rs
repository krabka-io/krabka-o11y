/// Reads an eight-byte span id as a big-endian `u64`.
///
/// This is the one place the byte order of a span id is decided. Traces hold a
/// span id as its eight bytes, profiles hold the same id as a `u64`, and the
/// two agree only while both read those bytes big-endian.
#[must_use]
pub const fn span_id_u64_from_be_bytes(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}
