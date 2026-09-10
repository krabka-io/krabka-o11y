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

mod span_id_be_bytes_from_u64;
mod span_id_hex_from_u64;
mod span_id_u64_from_be_bytes;
mod span_id_u64_from_be_slice;

pub use span_id_be_bytes_from_u64::span_id_be_bytes_from_u64;
pub use span_id_hex_from_u64::span_id_hex_from_u64;
pub use span_id_u64_from_be_bytes::span_id_u64_from_be_bytes;
pub use span_id_u64_from_be_slice::span_id_u64_from_be_slice;
