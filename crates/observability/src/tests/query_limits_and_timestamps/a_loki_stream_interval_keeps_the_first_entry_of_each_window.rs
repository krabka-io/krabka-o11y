use super::*;

/// `apply_loki_stream_interval` thins a stream so consecutive entries are at
/// least `interval` apart, keeping the first of each window. The entries
/// straddle the boundary deliberately: one exactly AT the next allowed
/// timestamp must be kept, since the comparison is `<` and not `<=`.
///
/// An entry whose timestamp will not parse is KEPT rather than dropped --
/// thinning is a display convenience, and silently discarding a line
/// because its timestamp is odd would lose data the user asked for.
#[test]
pub(crate) fn a_loki_stream_interval_keeps_the_first_entry_of_each_window() {
    let stream = |timestamps: &[&str]| {
        serde_json::json!({
            "data": {"result": [{
                "stream": {"app": "api"},
                "values": timestamps
                    .iter()
                    .map(|ts| serde_json::json!([ts, "line"]))
                    .collect::<Vec<_>>(),
            }]}
        })
    };
    let kept = |mut value: serde_json::Value, interval| {
        super::super::prelude::apply_loki_stream_interval(&mut value, interval);
        value
            .pointer("/data/result")
            .and_then(serde_json::Value::as_array)
            .map(|streams| {
                streams
                    .iter()
                    .flat_map(|s| s["values"].as_array().cloned().unwrap_or_default())
                    .map(|entry| entry[0].as_str().expect("a timestamp").to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };

    // Ten apart: the first is kept, then everything until ten past it.
    // 10 lands exactly on the boundary and is kept.
    check!(
        kept(stream(&["0", "5", "10", "15", "20"]), Some(10)) == vec!["0", "10", "20"],
        "an entry exactly at the boundary is kept"
    );
    check!(
        kept(stream(&["0", "9"]), Some(10)) == vec!["0"],
        "one short is dropped"
    );

    // No interval, and a zero interval, both leave the stream alone.
    check!(kept(stream(&["0", "1", "2"]), None) == vec!["0", "1", "2"]);
    check!(kept(stream(&["0", "1", "2"]), Some(0)) == vec!["0", "1", "2"]);

    // The zero-interval short circuit only shows on DESCENDING timestamps,
    // which is how Loki returns log entries. Without it a zero interval
    // sets the next allowed timestamp to the current one, and every later
    // entry compares as earlier and is dropped.
    check!(
        kept(stream(&["2", "1", "0"]), Some(0)) == vec!["2", "1", "0"],
        "a zero interval thins nothing, even newest-first"
    );

    // An unparseable timestamp is kept, and does not move the window.
    check!(
        kept(stream(&["0", "nonsense", "5"]), Some(10)) == vec!["0", "nonsense"],
        "the odd entry is kept and 5 is still inside the window"
    );

    // A stream thinned to nothing cannot happen -- the first entry always
    // survives -- but a stream that was already empty is dropped.
    let mut empty = serde_json::json!({
        "data": {"result": [{"stream": {}, "values": []}]}
    });
    super::super::prelude::apply_loki_stream_interval(&mut empty, Some(10));
    check!(
        empty["data"]["result"]
            .as_array()
            .expect("an array")
            .is_empty(),
        "an empty stream is dropped rather than sent"
    );
}
