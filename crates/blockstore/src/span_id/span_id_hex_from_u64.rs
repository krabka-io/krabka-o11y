/// Renders a `u64` span id as sixteen lowercase hex digits.
///
/// The digits are the big-endian bytes of the id, so this string is what
/// `hex::encode` gives for the same span id held as `[u8; 8]`. The leading
/// zeroes are kept: a span id is a fixed eight-byte identifier, and an id
/// printed short does not match the id a trace carries.
#[must_use]
pub fn span_id_hex_from_u64(span_id: u64) -> String {
    format!("{span_id:016x}")
}
