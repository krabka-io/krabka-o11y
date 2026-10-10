//! Which flag values the shared parsers accept and which they refuse.

use assert2::assert;
use krabka_units::convert::ByteSizeExt as _;

use super::{ByteSize, parse_positive_usize, parse_positive_whole_byte_size};

#[test]
fn positive_usize_accepts_one_and_refuses_zero_and_text() {
    assert!(parse_positive_usize("1") == Ok(1));
    assert!(
        parse_positive_usize("0")
            == Err("the value must be greater than 0, but received 0".to_owned())
    );
    assert!(parse_positive_usize("many").is_err());
}

#[test]
fn positive_whole_byte_size_refuses_fractions_and_sizes_past_two_to_the_53() {
    assert!(parse_positive_whole_byte_size("4KiB") == Ok(ByteSize::from_bytes(4096)));
    assert!(
        parse_positive_whole_byte_size("9007199254740992B")
            == Ok(ByteSize::from_bytes(9_007_199_254_740_992))
    );
    let refusal =
        Err("size must be a positive whole-byte value exactly representable by UOM".to_owned());
    assert!(parse_positive_whole_byte_size("1.5B") == refusal);
    assert!(parse_positive_whole_byte_size("9007199254740994B") == refusal);
    assert!(parse_positive_whole_byte_size("0").is_err());
}
