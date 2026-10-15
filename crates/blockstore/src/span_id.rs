//! Span-id conversion between the two representations Krabka holds.
//!
//! `krabka-traces` holds a span id as the eight bytes OTLP carries;
//! `krabka-profiles` holds the same id as the `u64` its `span_id` Arrow column
//! stores. Both readings are big-endian, and every conversion between them
//! goes through this module so that only one line of code decides that. A
//! reversed id matches nothing, and a query that selects nothing still returns
//! a well-formed empty answer, so the disagreement would be silent.

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The span id from the W3C `traceparent` example. Its leading zero byte
    /// is the one that a hex rendering drops when it is not padded, and its
    /// bytes read differently in each direction, so a reversed convention
    /// cannot pass.
    const TRACEPARENT_SPAN_BYTES: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
    const TRACEPARENT_SPAN_U64: u64 = 0x00f0_67aa_0ba9_02b7;

    #[test]
    fn a_span_id_reads_big_endian_in_both_directions() {
        check!(span_id_u64_from_be_bytes(TRACEPARENT_SPAN_BYTES) == TRACEPARENT_SPAN_U64);
        check!(span_id_be_bytes_from_u64(TRACEPARENT_SPAN_U64) == TRACEPARENT_SPAN_BYTES);
        check!(span_id_u64_from_be_slice(&TRACEPARENT_SPAN_BYTES) == Some(TRACEPARENT_SPAN_U64));
        check!(span_id_hex_from_u64(TRACEPARENT_SPAN_U64) == "00f067aa0ba902b7");
    }

    #[test]
    fn a_hex_span_id_keeps_its_leading_zeroes() {
        check!(span_id_hex_from_u64(0) == "0000000000000000");
        check!(span_id_hex_from_u64(42) == "000000000000002a");
        check!(span_id_hex_from_u64(u64::MAX) == "ffffffffffffffff");
    }

    #[test]
    fn a_span_id_that_is_not_eight_bytes_is_rejected() {
        check!(span_id_u64_from_be_slice(&[]).is_none());
        check!(span_id_u64_from_be_slice(&[0; 7]).is_none());
        check!(span_id_u64_from_be_slice(&[0; 9]).is_none());
        check!(span_id_u64_from_be_slice(&[0; 16]).is_none());
        check!(span_id_u64_from_be_slice(&[0; 8]) == Some(0));
    }
}

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

/// Writes a `u64` span id back as its eight big-endian bytes.
///
/// The inverse of [`span_id_u64_from_be_bytes`](span_id_u64_from_be_bytes).
#[must_use]
pub const fn span_id_be_bytes_from_u64(span_id: u64) -> [u8; 8] {
    span_id.to_be_bytes()
}

/// Reads an eight-byte span id as a big-endian `u64`.
///
/// This is the one place the byte order of a span id is decided. Traces hold a
/// span id as its eight bytes, profiles hold the same id as a `u64`, and the
/// two agree only while both read those bytes big-endian.
#[must_use]
pub const fn span_id_u64_from_be_bytes(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}
