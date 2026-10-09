use std::hash::{BuildHasher, RandomState};

use assert2::assert;
use krabka_metrics::MetricString;

#[test]
fn owned_utf8_reuses_one_buffer_and_byte_strings_keep_their_wire_identity() {
    let cases = [
        (Vec::new(), ""),
        (b"temperature".to_vec(), "temperature"),
        ("a\0é🦀".as_bytes().to_vec(), "a\0é🦀"),
        (vec![0xff], "\u{fffd}"),
        (vec![0xfe], "\u{fffd}"),
        (vec![0xe2, 0x82], "\u{fffd}\u{fffd}"),
        (b"a\xff\xc3\xa9\xfez".to_vec(), "a\u{fffd}é\u{fffd}z"),
    ];
    let hash = RandomState::new();
    for (bytes, json) in cases {
        let input = bytes.clone();
        let allocation = input.as_ptr();
        let value = MetricString::from(input);
        assert!(value.as_bytes() == bytes && value.as_str() == json);
        assert!(value.as_bytes().as_ptr() == allocation);
        assert!(value.utf8() == std::str::from_utf8(&bytes).ok());
        assert!(hash.hash_one(&value) == hash.hash_one(&bytes));
        assert!(value.clone() == value);
        if let Some(text) = value.utf8() {
            assert!(value.as_bytes().as_ptr() == value.as_str().as_ptr());
            let owned = text.to_owned();
            let allocation = owned.as_ptr();
            let from_string = MetricString::from(owned);
            assert!(from_string == value && from_string.as_bytes().as_ptr() == allocation);
            assert!(serde_json::to_value(&value).unwrap() == serde_json::json!(text));
        } else {
            assert!(serde_json::to_value(&value).unwrap() == serde_json::json!(bytes));
        }
        let encoded =
            <serde_wincode::SerdeCompat<MetricString> as wincode::Serialize>::serialize(&value)
                .unwrap();
        let expected =
            <serde_wincode::SerdeCompat<Vec<u8>> as wincode::Serialize>::serialize(&bytes).unwrap();
        assert!(encoded == expected);
        let restored =
            <serde_wincode::SerdeCompat<MetricString> as wincode::Deserialize>::deserialize(
                &encoded,
            )
            .unwrap();
        assert!(restored == value && restored.as_str() == json);
        let cached = serde_json::to_value(&value).unwrap();
        assert!(serde_json::from_value::<MetricString>(cached).unwrap() == value);
    }

    let raw = MetricString::from(b"a\xff\xc3\xa9\xfez".to_vec());
    for (start, end, expected) in [
        (0, 1, Some(&b"a"[..])),
        (1, 4, Some(&b"\xff"[..])),
        (2, 4, None),
        (4, 6, Some(&b"\xc3\xa9"[..])),
        (6, 9, Some(&b"\xfe"[..])),
        (9, 10, Some(&b"z"[..])),
        (10, 10, Some(&b""[..])),
        (10, 11, None),
    ] {
        assert!(raw.bytes_for_json_range(start, end) == expected);
    }
    assert!(MetricString::from(vec![0xff]) != MetricString::from(vec![0xfe]));
    assert!(MetricString::from(vec![0xff]) != MetricString::from("\u{fffd}"));
    assert!(raw.quoted() == "\"a\\xff\\xc3\\xa9\\xfez\"");
    let bytes = [b"a".to_vec(), vec![0xff], "\u{fffd}".as_bytes().to_vec()];
    let mut expected = bytes.to_vec();
    expected.sort();
    let mut actual = bytes.map(MetricString::from).to_vec();
    actual.sort();
    assert!(
        actual
            .iter()
            .map(MetricString::as_bytes)
            .collect::<Vec<_>>()
            == expected
    );
}
