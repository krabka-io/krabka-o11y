use super::*;

/// Structured metadata reaches the pipeline as fields, which is how a filter
/// such as `| trace_id="abc"` matches on it, and Loki's default encoding leaves
/// it there: the metadata is folded into the stream's label map, so two rows
/// that differ only by `trace_id` come back as two streams of one entry each,
/// and every entry stays two elements long.
///
/// `categorize-labels` is the encoding that pulls the metadata back out. The
/// two streams become one again -- the stream Grafana's log browser shows --
/// and each entry names what it carried. `detected_level` travels with the
/// metadata, because that is the category Loki's level discovery writes it in.
#[test]
pub(crate) fn structured_metadata_splits_a_stream_unless_the_request_categorizes_labels() {
    let mut label_index = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let api = label_index.insert_series("tenant", labels);

    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 100).expect("a valid range"),
        query: parse_query("{app=\"api\"}").expect("the query parses"),
        fingerprints: [api].into_iter().collect(),
        blocks: Vec::new(),
    };

    let mut streams = BTreeMap::new();
    for (timestamp_ns, trace_id) in [(10_i64, "abc"), (20, "def")] {
        let mut structured_metadata = Labels::default();
        structured_metadata.insert("trace_id".to_string(), trace_id.to_string());
        append_matching_log_row(
            &mut streams,
            &plan,
            &label_index,
            QueryRow {
                fingerprint: api,
                timestamp_ns,
                line: "api error",
                structured_metadata: &structured_metadata,
            },
            &[],
        )
        .expect("the plan names the series");
    }

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
                                "trace_id": "abc"
                            },
                            "values": [["10", "api error"]]
                        },
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "trace_id": "def"
                            },
                            "values": [["20", "api error"]]
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
                                    "api error",
                                    {
                                        "structuredMetadata": {
                                            "detected_level": "unknown",
                                            "trace_id": "abc"
                                        }
                                    }
                                ],
                                [
                                    "20",
                                    "api error",
                                    {
                                        "structuredMetadata": {
                                            "detected_level": "unknown",
                                            "trace_id": "def"
                                        }
                                    }
                                ]
                            ]
                        }
                    ]
                }
            })
    );
}
