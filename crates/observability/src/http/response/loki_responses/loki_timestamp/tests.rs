use std::collections::HashSet;

use super::{
    super::{Labels, LokiStreamEntry, json},
    LokiTimestamp,
};

#[test]
fn timestamps_keep_their_exact_wire_spelling() {
    let mut metadata = Labels::new();
    metadata.insert("metadata".into(), "λ 🎉".into());
    let mut parsed = Labels::new();
    parsed.insert("parsed".into(), "\\\"\n".into());
    let line = "line\\\"\n λ 🎉";
    struct TimestampCase {
        text: &'static str,
        expected_ns: Option<i64>,
    }
    for case in [
        TimestampCase {
            text: "-9223372036854775808",
            expected_ns: Some(i64::MIN),
        },
        TimestampCase {
            text: "-1",
            expected_ns: Some(-1),
        },
        TimestampCase {
            text: "0",
            expected_ns: Some(0),
        },
        TimestampCase {
            text: "1",
            expected_ns: Some(1),
        },
        TimestampCase {
            text: "1700000000000000000",
            expected_ns: Some(1_700_000_000_000_000_000),
        },
        TimestampCase {
            text: "9223372036854775807",
            expected_ns: Some(i64::MAX),
        },
        TimestampCase {
            text: "01",
            expected_ns: Some(1),
        },
        TimestampCase {
            text: "+1",
            expected_ns: Some(1),
        },
        TimestampCase {
            text: "-0",
            expected_ns: Some(0),
        },
        TimestampCase {
            text: "",
            expected_ns: None,
        },
        TimestampCase {
            text: "not-a-timestamp",
            expected_ns: None,
        },
        TimestampCase {
            text: "9223372036854775808",
            expected_ns: None,
        },
        TimestampCase {
            text: "-9223372036854775809",
            expected_ns: None,
        },
        TimestampCase {
            text: "1 λ 🎉",
            expected_ns: None,
        },
        TimestampCase {
            text: "\"\\\n",
            expected_ns: None,
        },
    ] {
        let mut entry = LokiStreamEntry::new(0, line.into(), metadata.clone(), parsed.clone());
        entry.timestamp_ns = case.text.into();
        assert2::assert!(entry.parsed_timestamp_ns() == case.expected_ns);
        assert2::assert!(serde_json::to_value(&entry).unwrap() == json!([case.text, line]));
        assert2::assert!(
            entry.categorized_value()
                == json!([case.text, line, {"structuredMetadata": metadata, "parsed": parsed}])
        );
        assert2::assert!(String::from(entry.timestamp_ns.clone()) == case.text);
    }
}

#[test]
fn canonical_numbers_share_identity_with_their_text() {
    for value in [i64::MIN, -1, 0, 1, 1_700_000_000_000_000_000, i64::MAX] {
        let numeric = LokiTimestamp::from(value);
        let textual = LokiTimestamp::from(value.to_string());
        assert2::assert!(HashSet::from([numeric.clone(), textual]) == HashSet::from([numeric]));
        let entry = LokiStreamEntry::new(value, "line".into(), Labels::new(), Labels::new());
        assert2::assert!(
            serde_json::to_value(&entry).unwrap() == json!([value.to_string(), "line"])
        );
        assert2::assert!(entry.categorized_value() == json!([value.to_string(), "line", {}]));
    }
    let keys = HashSet::from([
        LokiTimestamp::from(1),
        LokiTimestamp::from("1"),
        LokiTimestamp::from("01"),
        LokiTimestamp::from("+1"),
    ]);
    let mut strings = keys.into_iter().map(String::from).collect::<Vec<_>>();
    strings.sort();
    assert2::assert!(strings == vec!["+1", "01", "1"]);
}
