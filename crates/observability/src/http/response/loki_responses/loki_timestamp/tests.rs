use std::collections::HashSet;

use super::{
    super::{Labels, LokiStreamEntry, json},
    LokiTimestamp,
};

#[test]
fn timestamps_keep_their_exact_wire_spelling() {
    let metadata = Labels::from([("metadata".into(), "λ 🎉".into())]);
    let parsed = Labels::from([("parsed".into(), "\\\"\n".into())]);
    let line = "line\\\"\n λ 🎉";
    for (text, expected) in [
        ("-9223372036854775808", Some(i64::MIN)),
        ("-1", Some(-1)),
        ("0", Some(0)),
        ("1", Some(1)),
        ("1700000000000000000", Some(1_700_000_000_000_000_000)),
        ("9223372036854775807", Some(i64::MAX)),
        ("01", Some(1)),
        ("+1", Some(1)),
        ("-0", Some(0)),
        ("", None),
        ("not-a-timestamp", None),
        ("9223372036854775808", None),
        ("-9223372036854775809", None),
        ("1 λ 🎉", None),
        ("\"\\\n", None),
    ] {
        let mut entry = LokiStreamEntry::new(0, line.into(), metadata.clone(), parsed.clone());
        entry.timestamp_ns = text.into();
        assert2::assert!(
            (
                entry.parsed_timestamp_ns(),
                serde_json::to_value(&entry).unwrap(),
                entry.categorized_value(),
                String::from(entry.timestamp_ns.clone()),
            ) == (
                expected,
                json!([text, line]),
                json!([text, line, {"structuredMetadata": metadata, "parsed": parsed}]),
                text.to_owned(),
            )
        );
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
            (
                serde_json::to_value(&entry).unwrap(),
                entry.categorized_value()
            ) == (
                json!([value.to_string(), "line"]),
                json!([value.to_string(), "line", {}])
            )
        );
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
