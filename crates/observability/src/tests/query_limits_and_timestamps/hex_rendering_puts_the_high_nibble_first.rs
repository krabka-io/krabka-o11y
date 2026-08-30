use super::*;

/// `hex_string` renders bytes as lower-case hex, high nibble first. The
/// byte 0xAB is the case that matters: with a symmetric byte like 0xAA a
/// swapped nibble order is invisible.
#[test]
pub(crate) fn hex_rendering_puts_the_high_nibble_first() {
    let hex = super::super::prelude::hex_string;

    check!(hex(&[0xAB]) == "ab", "high nibble first");
    check!(hex(&[0x0F]) == "0f", "a leading zero is kept");
    check!(hex(&[0xF0]) == "f0");
    check!(hex(&[0x00]) == "00");
    check!(hex(&[0xFF]) == "ff");
    check!(hex(&[0x01, 0x23]) == "0123", "bytes in order");
    check!(hex(&[]) == "");
    check!(hex(&[0xDE, 0xAD, 0xBE, 0xEF]) == "deadbeef");
}
