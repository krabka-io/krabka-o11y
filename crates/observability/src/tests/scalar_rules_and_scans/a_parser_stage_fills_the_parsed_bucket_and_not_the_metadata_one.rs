use super::*;

/// A label a parser stage produced is a category of its own upstream. The
/// default encoding folds it into the stream's labels alongside the structured
/// metadata, so the two are indistinguishable there; under `categorize-labels`
/// it lands in `parsed` while the pushed metadata stays in
/// `structuredMetadata`, and the stream keeps only the series' own labels.
#[test]
pub(crate) fn a_parser_stage_fills_the_parsed_bucket_and_not_the_metadata_one() {
    let mut label_index = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let api = label_index.insert_series("tenant", labels);

    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 100).expect("a valid range"),
        query: parse_query("{app=\"api\"} | json").expect("the query parses"),
        fingerprints: [api].into_iter().collect(),
        blocks: Vec::new(),
    };

    let mut structured_metadata = Labels::default();
    structured_metadata.insert("trace_id".to_string(), "abc".to_string());
    let mut streams = BTreeMap::new();
    append_matching_log_row(
        &mut streams,
        &plan,
        &label_index,
        QueryRow {
            fingerprint: api,
            timestamp_ns: 10,
            line: r#"{"status":"500"}"#,
            structured_metadata: &structured_metadata,
        },
        &[],
    )
    .expect("the plan names the series");

    check!(
        loki_streams_response(streams.clone(), LokiStreamEncoding::Folded)
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "status": "500",
                                "trace_id": "abc"
                            },
                            "values": [["10", r#"{"status":"500"}"#]]
                        }
                    ]
                }
            })
    );

    check!(
        loki_streams_response(streams, LokiStreamEncoding::CategorizeLabels)
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {"app": "api"},
                            "values": [
                                [
                                    "10",
                                    r#"{"status":"500"}"#,
                                    {
                                        "structuredMetadata": {
                                            "detected_level": "unknown",
                                            "trace_id": "abc"
                                        },
                                        "parsed": {"status": "500"}
                                    }
                                ]
                            ]
                        }
                    ]
                }
            })
    );
}
