//! Round-trip properties for the span-id conversion the signals share.
//!
//! `krabka-traces` holds a span id as `[u8; 8]` and `krabka-profiles` holds it
//! as a `u64`. The properties below show the two readings are inverse, and the
//! fixed vector below them shows which reading it is: a consistently reversed
//! convention round-trips just as happily, and would then match nothing at all
//! against an id that came in over OTLP.

use std::fmt::Write as _;

use assert2::check;
use krabka_blockstore::{
    span_id_be_bytes_from_u64, span_id_hex_from_u64, span_id_u64_from_be_bytes,
    span_id_u64_from_be_slice,
};
use proptest::prelude::*;

/// Lowercase hex, one byte at a time, built without the conversion under test.
fn hex_of(bytes: [u8; 8]) -> String {
    let mut out = String::with_capacity(16);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("a String write cannot fail");
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn a_u64_span_id_survives_a_trip_through_its_bytes(span_id: u64) {
        let bytes = span_id_be_bytes_from_u64(span_id);
        prop_assert!(span_id_u64_from_be_bytes(bytes) == span_id);
        prop_assert!(span_id_u64_from_be_slice(&bytes) == Some(span_id));
    }

    #[test]
    fn span_id_bytes_survive_a_trip_through_a_u64(bytes: [u8; 8]) {
        let span_id = span_id_u64_from_be_bytes(bytes);
        prop_assert!(span_id_be_bytes_from_u64(span_id) == bytes);
    }

    /// The `u64` an id is stored as and the bytes a trace carries print the
    /// same sixteen hex digits. This is the form both signals expose over
    /// HTTP, so an unpadded rendering is an id that no trace can be found by.
    #[test]
    fn a_hex_span_id_is_the_hex_of_its_bytes(bytes: [u8; 8]) {
        let hex = span_id_hex_from_u64(span_id_u64_from_be_bytes(bytes));
        prop_assert!(hex == hex_of(bytes));
        prop_assert!(hex.len() == 16);
    }

    /// Only an id whose bytes read the same backwards can survive a reversal.
    /// Everything else must change, which is what makes the direction of the
    /// conversion observable rather than a matter of convention.
    #[test]
    fn reading_span_id_bytes_backwards_changes_the_id(bytes: [u8; 8]) {
        let mut reversed = bytes;
        reversed.reverse();
        prop_assume!(reversed != bytes);
        prop_assert!(span_id_u64_from_be_bytes(reversed) != span_id_u64_from_be_bytes(bytes));
    }
}

/// The span id of the `traceparent` example in the W3C trace-context
/// specification, which is also the id OTLP puts on the wire byte for byte.
///
/// Its leading zero byte and its unequal ends pin both the padding and the
/// byte order, neither of which a round trip on its own can see.
#[test]
fn the_w3c_traceparent_span_id_reads_big_endian() {
    const BYTES: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
    const ID: u64 = 0x00f0_67aa_0ba9_02b7;
    const HEX: &str = "00f067aa0ba902b7";

    check!(span_id_u64_from_be_bytes(BYTES) == ID);
    check!(span_id_u64_from_be_slice(&BYTES) == Some(ID));
    check!(span_id_be_bytes_from_u64(ID) == BYTES);
    check!(span_id_hex_from_u64(ID) == HEX);

    // Little-endian would read this id as `0xb702a90baa67f000`, and the query
    // that used it would select nothing while reporting success.
    check!(u64::from_le_bytes(BYTES) != ID);
}
