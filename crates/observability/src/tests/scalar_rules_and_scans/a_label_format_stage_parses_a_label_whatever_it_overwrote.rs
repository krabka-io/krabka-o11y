use super::*;

/// `label_format` is the one stage whose write cannot be recognised from the
/// value it left behind: it can name a series label, a piece of structured
/// metadata, or a label that did not exist, and it can write any of them the
/// string that was already there. Loki categorises all four the same way --
/// the destination is parsed, and the name leaves whichever bucket held it.
///
/// `env` here is rewritten to the value the series already carried, which is
/// the case a value-based reading gets wrong, and `shard` is a metadata name
/// the stage takes over.
#[test]
pub(crate) fn a_label_format_stage_parses_a_label_whatever_it_overwrote() {
    let mut label_index = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    labels.insert("env".to_string(), "prod".to_string());
    let api = label_index.insert_series("tenant", labels);

    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 100).expect("a valid range"),
        query: parse_query("{app=\"api\"} | label_format env=\"prod\", shard=\"z\", region=\"eu\"")
            .expect("the query parses"),
        fingerprints: [api].into_iter().collect(),
        blocks: Vec::new(),
    };

    let mut structured_metadata = Labels::default();
    structured_metadata.insert("shard".to_string(), "a".to_string());
    let mut streams = BTreeMap::new();
    append_matching_log_row(
        &mut streams,
        &plan,
        &label_index,
        QueryRow {
            fingerprint: api,
            timestamp_ns: 10,
            line: "api ok",
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
                                "env": "prod",
                                "region": "eu",
                                "shard": "z"
                            },
                            "values": [["10", "api ok"]]
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
                                    "api ok",
                                    {
                                        "structuredMetadata": {"detected_level": "unknown"},
                                        "parsed": {
                                            "env": "prod",
                                            "region": "eu",
                                            "shard": "z"
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
