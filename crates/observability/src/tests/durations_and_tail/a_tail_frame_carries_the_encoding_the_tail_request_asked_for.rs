use super::*;

/// A tail frame is a `streams` answer without the query envelope, and it
/// carries the same two encodings. The default folds an entry's structured
/// metadata into the stream's labels and leaves the entry two elements long;
/// `categorize-labels` keeps the stream to its own labels and gives the entry
/// the envelope.
///
/// Loki is not consistent here -- the frame it backfills when a tail opens
/// folds, the frames it streams afterwards drop the metadata instead -- so
/// Krabka folds throughout, which is the backfill's answer.
#[test]
pub(crate) fn a_tail_frame_carries_the_encoding_the_tail_request_asked_for() {
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let mut structured_metadata = BTreeMap::new();
    structured_metadata.insert("trace_id".to_string(), "abc".to_string());
    let record = WalLogRecord {
        tenant: "tenant".to_string(),
        labels,
        timestamp_ns: 10,
        line: "api error".to_string(),
        structured_metadata,
        position: None,
    };
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant", record.labels.clone());
    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 100).expect("a valid range"),
        query: parse_query("{app=\"api\"}").expect("the query parses"),
        fingerprints: [api].into_iter().collect(),
        blocks: Vec::new(),
    };
    let frontier = CompactionFrontier::new(0);
    let frame = |encoding| {
        execute_tail_query_with_frontier_and_deletes(
            &plan,
            std::slice::from_ref(&record),
            &frontier,
            &[],
            encoding,
        )
    };

    check!(
        frame(LokiStreamEncoding::Folded)
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "trace_id": "abc"
                        },
                        "values": [["10", "api error"]]
                    }
                ]
            })
    );

    check!(
        frame(LokiStreamEncoding::CategorizeLabels)
            == json!({
                "streams": [
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
                            ]
                        ]
                    }
                ]
            })
    );
}
