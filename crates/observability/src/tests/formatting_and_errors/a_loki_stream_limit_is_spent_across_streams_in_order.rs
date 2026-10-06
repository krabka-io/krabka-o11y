use super::*;

/// A single global budget selects the earliest entries across overlapping streams.
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
        kept(&super::super::prelude::apply_loki_stream_limit(
            streams(&[2, 10]),
            crate::http::params::value_decoding::LokiDirection::Forward,
            Some(5)
        )) == vec![2, 3]
    );

    // Overlapping timestamps interleave both streams within the global cap.
    check!(
        kept(&super::super::prelude::apply_loki_stream_limit(
            streams(&[5, 10]),
            crate::http::params::value_decoding::LokiDirection::Forward,
            Some(5)
        )) == vec![3, 2]
    );

    // Under budget, nothing is touched.
    check!(
        kept(&super::super::prelude::apply_loki_stream_limit(
            streams(&[2, 2]),
            crate::http::params::value_decoding::LokiDirection::Forward,
            Some(5)
        )) == vec![2, 2]
    );

    // No limit means no truncation, and a non-streams result is left alone.
    check!(
        kept(&super::super::prelude::apply_loki_stream_limit(
            streams(&[9]),
            crate::http::params::value_decoding::LokiDirection::Forward,
            None
        )) == vec![9]
    );
}
