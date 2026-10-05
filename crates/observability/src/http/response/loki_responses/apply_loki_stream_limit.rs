use super::{LokiDirection, Value};

pub(crate) fn apply_loki_stream_limit(
    mut value: Value,
    direction: LokiDirection,
    limit: Option<usize>,
) -> Value {
    crate::query_frontend::apply_global_stream_limit(&mut value, direction, limit);
    value
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;

    #[test]
    fn limit_selects_entries_by_time_across_streams_and_preserves_metadata() {
        let response = json!({"status": "success", "data": {
            "resultType": "streams", "result": [
                {"stream": {"app": "a"}, "values": [
                    ["10", "a10", {"structuredMetadata": {"source": "first"}}],
                    ["30", "a30", {"parsed": {"level": "error"}}]
                ]},
                {"stream": {"app": "b"}, "values": [
                    ["20", "b20"], ["40", "b40"]
                ]}
            ]
        }});
        let mut expected = response.clone();
        expected["data"]["result"][1]["values"] = json!([["20", "b20"]]);
        assert!(
            apply_loki_stream_limit(response.clone(), LokiDirection::Forward, Some(3)) == expected
        );

        let mut backwards = response;
        for stream in backwards["data"]["result"].as_array_mut().unwrap() {
            stream["values"].as_array_mut().unwrap().reverse();
        }
        let mut expected = backwards.clone();
        expected["data"]["result"][0]["values"] =
            json!([["30", "a30", {"parsed": {"level": "error"}}]]);
        assert!(apply_loki_stream_limit(backwards, LokiDirection::Backward, Some(3)) == expected);
    }

    #[test]
    fn equal_timestamp_ties_keep_label_order_in_both_directions() {
        let response = json!({"data": {"resultType": "streams", "result": [
            {"stream": {"app": "a"}, "values": [["10", "a"]]},
            {"stream": {"app": "b"}, "values": [["10", "b"]]}
        ]}});
        let expected = json!({"data": {"resultType": "streams", "result": [
            {"stream": {"app": "a"}, "values": [["10", "a"]]}
        ]}});
        for direction in [LokiDirection::Forward, LokiDirection::Backward] {
            assert!(apply_loki_stream_limit(response.clone(), direction, Some(1)) == expected);
            assert!(apply_loki_stream_limit(response.clone(), direction, None) == response);
            let empty = apply_loki_stream_limit(response.clone(), direction, Some(0));
            assert!(empty["data"]["result"] == json!([]));
        }
        let metrics = json!({"data": {"resultType": "matrix", "result": [
            {"metric": {"app": "a"}, "values": [[10, "2"]]}
        ]}});
        assert!(
            apply_loki_stream_limit(metrics.clone(), LokiDirection::Backward, Some(0)) == metrics
        );
    }
}
