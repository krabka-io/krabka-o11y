use super::*;

/// `apply_loki_stream_limit` spends one budget across several streams,
/// truncating the last one that fits. The budget only visibly decrements
/// when an earlier stream takes part of it and a later stream needs the
/// rest -- with a single stream, any arithmetic on the remainder looks
/// alike.
#[test]
pub(crate) fn a_loki_stream_limit_is_spent_across_streams_in_order() {
    let streams = |counts: &[usize]| {
        serde_json::json!({
            "data": {
                "resultType": "streams",
                "result": counts
                    .iter()
                    .map(|count| serde_json::json!({
                        "stream": {"app": "a"},
                        "values": (0..*count)
                            .map(|i| serde_json::json!([i.to_string(), "line"]))
                            .collect::<Vec<_>>(),
                    }))
                    .collect::<Vec<_>>(),
            }
        })
    };
    let kept = |value: &serde_json::Value| {
        value
            .pointer("/data/result")
            .and_then(serde_json::Value::as_array)
            .expect("the result is an array")
            .iter()
            .map(|stream| {
                stream
                    .get("values")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len)
            })
            .collect::<Vec<_>>()
    };

    // The first stream takes 2 of the 5, leaving 3 for the second.
    // Adding instead would leave 7, and dividing would leave 2.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[2, 10]),
            Some(5)
        )) == vec![2, 3]
    );

    // A stream that exhausts the budget empties every stream after it,
    // and emptied streams are dropped entirely.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[5, 10]),
            Some(5)
        )) == vec![5]
    );

    // Under budget, nothing is touched.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[2, 2]),
            Some(5)
        )) == vec![2, 2]
    );

    // No limit means no truncation, and a non-streams result is left alone.
    check!(
        kept(&super::prelude::apply_loki_stream_limit(
            streams(&[9]),
            None
        )) == vec![9]
    );
}
