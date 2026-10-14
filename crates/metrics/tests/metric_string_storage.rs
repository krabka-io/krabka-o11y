use std::hash::{BuildHasher, RandomState};

use assert2::assert;
use krabka_metrics::MetricString;

struct StringCase {
    bytes: Vec<u8>,
    json: &'static str,
}

struct ByteRangeCase {
    start: usize,
    end: usize,
    expected: Option<&'static [u8]>,
}

#[test]
fn owned_utf8_reuses_one_buffer_and_byte_strings_keep_their_wire_identity() {
    let cases = [
        StringCase {
            bytes: Vec::new(),
            json: "",
        },
        StringCase {
            bytes: b"temperature".to_vec(),
            json: "temperature",
        },
        StringCase {
            bytes: "a\0é🦀".as_bytes().to_vec(),
            json: "a\0é🦀",
        },
        StringCase {
            bytes: vec![0xff],
            json: "\u{fffd}",
        },
        StringCase {
            bytes: vec![0xfe],
            json: "\u{fffd}",
        },
        StringCase {
            bytes: vec![0xe2, 0x82],
            json: "\u{fffd}\u{fffd}",
        },
        StringCase {
            bytes: b"a\xff\xc3\xa9\xfez".to_vec(),
            json: "a\u{fffd}é\u{fffd}z",
        },
    ];
    let hash = RandomState::new();
    for StringCase { bytes, json } in cases {
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
    for ByteRangeCase {
        start,
        end,
        expected,
    } in [
        ByteRangeCase {
            start: 0,
            end: 1,
            expected: Some(&b"a"[..]),
        },
        ByteRangeCase {
            start: 1,
            end: 4,
            expected: Some(&b"\xff"[..]),
        },
        ByteRangeCase {
            start: 2,
            end: 4,
            expected: None,
        },
        ByteRangeCase {
            start: 4,
            end: 6,
            expected: Some(&b"\xc3\xa9"[..]),
        },
        ByteRangeCase {
            start: 6,
            end: 9,
            expected: Some(&b"\xfe"[..]),
        },
        ByteRangeCase {
            start: 9,
            end: 10,
            expected: Some(&b"z"[..]),
        },
        ByteRangeCase {
            start: 10,
            end: 10,
            expected: Some(&b""[..]),
        },
        ByteRangeCase {
            start: 10,
            end: 11,
            expected: None,
        },
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
