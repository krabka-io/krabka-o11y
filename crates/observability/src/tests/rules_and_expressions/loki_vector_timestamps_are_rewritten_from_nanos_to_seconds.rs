use super::*;

/// `normalize_loki_vector_sample_timestamps_to_seconds` rewrites each
/// instant sample's timestamp from nanoseconds to seconds in place. It
/// accepts the timestamp as a JSON number OR a string, since both spellings
/// reach it, and it writes back a whole number when the nanos divide
/// exactly and a float otherwise -- a client parsing "1700000000" as an
/// integer must not be handed "1700000000.0".
#[test]
pub(crate) fn loki_vector_timestamps_are_rewritten_from_nanos_to_seconds() {
    let normalize = |timestamp: serde_json::Value| {
        let mut value = serde_json::json!({
            "data": {"result": [{"metric": {}, "value": [timestamp, "1"]}]}
        });
        super::super::prelude::normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
        value["data"]["result"][0]["value"][0].clone()
    };

    // An exact second becomes an integer, in both spellings.
    check!(normalize(serde_json::json!(1_700_000_000_000_000_000_u64)) == 1_700_000_000);
    check!(normalize(serde_json::json!("1700000000000000000")) == 1_700_000_000);

    // A fractional second becomes a float rather than being truncated.
    check!(normalize(serde_json::json!(1_500_000_000_u64)) == 1.5);
    check!(normalize(serde_json::json!("1500000000")) == 1.5);

    check!(normalize(serde_json::json!(0_u64)) == 0);

    // A timestamp that is neither a number nor a string is left alone
    // rather than replaced with a default.
    check!(normalize(serde_json::json!(true)) == true);

    // A response with no result array is left untouched.
    let mut empty = serde_json::json!({"status": "success"});
    let before = empty.clone();
    super::super::prelude::normalize_loki_vector_sample_timestamps_to_seconds(&mut empty);
    check!(empty == before);
}
