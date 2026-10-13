use super::*;

/// Loki's default JSON encoding writes every entry as `["<ns>", "<line>"]`,
/// whatever the entry carries: the labels it came with are folded into the
/// stream's label map instead, and `[ts, line, {...}]` is not a shape a default
/// client ever sees.
///
/// `categorize-labels` is the encoding that widens the entry, and the third
/// element it adds is an envelope naming each bucket -- never the bare metadata
/// map. An entry with nothing to categorise still gets the envelope, empty.
#[test]
pub(crate) fn a_stream_entry_is_two_elements_until_a_request_categorizes_labels() {
    let bare = LokiStreamEntry::new(
        19,
        "api error".to_string(),
        Labels::default(),
        Labels::default(),
    );
    check!(serde_json::to_value(&bare).expect("an entry serialises") == json!(["19", "api error"]));
    check!(bare.categorized_value() == json!(["19", "api error", {}]));

    let annotated = LokiStreamEntry::new(
        19,
        "api error".to_string(),
        krabka_blockstore::labels([
            krabka_blockstore::LabelPair::new("span_id", "def"),
            krabka_blockstore::LabelPair::new("trace_id", "abc"),
        ]),
        krabka_blockstore::labels([krabka_blockstore::LabelPair::new("status", "500")]),
    );
    check!(
        serde_json::to_value(&annotated).expect("an entry serialises")
            == json!(["19", "api error"])
    );
    check!(
        annotated.categorized_value()
            == json!([
                "19",
                "api error",
                {
                    "structuredMetadata": {"span_id": "def", "trace_id": "abc"},
                    "parsed": {"status": "500"}
                }
            ])
    );
}
