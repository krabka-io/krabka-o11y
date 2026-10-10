//! Which flag values the shared parsers accept and which they refuse.

use assert2::assert;
use krabka_units::convert::ByteSizeExt as _;

use super::{
    ByteSize, parse_client_dispatch_queue_capacity, parse_client_frame_max,
    parse_consumer_fetch_size, parse_min_two_usize, parse_positive_u32, parse_positive_usize,
    parse_positive_whole_byte_size,
};

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
fn positive_u32_accepts_one_and_refuses_zero_text_and_overflow() {
    assert!(parse_positive_u32("1") == Ok(1));
    assert!(parse_positive_u32("4294967295") == Ok(u32::MAX));
    assert!(parse_positive_u32("0") == Err("value must be at least 1".to_owned()));
    assert!(parse_positive_u32("many").is_err());
    assert!(parse_positive_u32("4294967296").is_err());
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

#[test]
fn min_two_usize_accepts_two_and_refuses_one_zero_and_text() {
    assert!(parse_min_two_usize("2") == Ok(2));
    assert!(parse_min_two_usize("1") == Err("value must be at least 2".to_owned()));
    assert!(parse_min_two_usize("0") == Err("value must be at least 2".to_owned()));
    assert!(parse_min_two_usize("many").is_err());
}

#[test]
fn client_dispatch_queue_capacity_accepts_a_count_and_refuses_text() {
    assert!(parse_client_dispatch_queue_capacity("7") == Ok(7));
    assert!(parse_client_dispatch_queue_capacity("many").is_err());
    assert!(parse_client_dispatch_queue_capacity("-1").is_err());
}

#[test]
fn client_frame_max_accepts_a_byte_size_and_refuses_zero_and_text() {
    assert!(parse_client_frame_max("32KiB") == Ok(ByteSize::from_bytes(32 * 1024)));
    assert!(parse_client_frame_max("0").is_err());
    assert!(parse_client_frame_max("many").is_err());
}

#[test]
fn consumer_fetch_size_refuses_sizes_past_a_kafka_fetch() {
    assert!(parse_consumer_fetch_size("1MiB") == Ok(ByteSize::from_bytes(1024 * 1024)));
    for refused in ["0", "not-a-number", "-1B", "1.5B", "2147483648B"] {
        assert!(parse_consumer_fetch_size(refused).is_err(), "{refused:?}");
    }
}
