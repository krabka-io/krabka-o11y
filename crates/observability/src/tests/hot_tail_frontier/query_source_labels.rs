use assert2::assert;

use super::*;
use crate::{
    LokiStreamEncoding, execute_stream_query_with_hot_tail_frontier_and_deletes,
    execute_tail_query_with_frontier_and_deletes,
};

struct WalRecordCase {
    app: &'static str,
    timestamp_ns: i64,
    status: &'static str,
}

struct StreamEncodingCase {
    encoding: LokiStreamEncoding,
    expected: serde_json::Value,
}

fn source_labels(app: &str) -> Labels {
    let mut labels = Labels::new();
    labels.insert("app".into(), app.into());
    labels.insert("method".into(), "GET".into());
    labels
}

#[tokio::test]
async fn source_labels_preserve_distinct_and_both_tail_encodings() {
    let records = [
        WalRecordCase {
            app: "api",
            timestamp_ns: 10,
            status: "200",
        },
        WalRecordCase {
            app: "api",
            timestamp_ns: 20,
            status: "500",
        },
        WalRecordCase {
            app: "web",
            timestamp_ns: 30,
            status: "200",
        },
    ]
    .map(|case| {
        let mut structured_metadata = BTreeMap::new();
        structured_metadata.insert("status".into(), case.status.into());
        WalLogRecord {
            tenant: "tenant".into(),
            labels: source_labels(case.app),
            timestamp_ns: case.timestamp_ns,
            line: format!("line-{}", case.timestamp_ns),
            structured_metadata,
            position: None,
        }
    });
    let original = records.clone();
    let mut plan = krabka_logql::StreamPlan {
        tenant: "tenant".into(),
        time_range: TimeRange::new(0, 100).unwrap(),
        query: krabka_logql::parse_query(r#"{app=~"api|web"}"#).unwrap(),
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let frontier = CompactionFrontier::new(0);
    let folded = json!([
        {"stream":{"app":"api","method":"GET","status":"200"},"values":[["10","line-10"]]},
        {"stream":{"app":"api","method":"GET","status":"500"},"values":[["20","line-20"]]},
        {"stream":{"app":"web","method":"GET","status":"200"},"values":[["30","line-30"]]}
    ]);
    let categorized = json!([
        {"stream":{"app":"api","method":"GET"},"values":[
            ["10","line-10",{"structuredMetadata":{"status":"200"}}],
            ["20","line-20",{"structuredMetadata":{"status":"500"}}]]},
        {"stream":{"app":"web","method":"GET"},"values":[
            ["30","line-30",{"structuredMetadata":{"status":"200"}}]]}
    ]);
    for case in [
        StreamEncodingCase {
            encoding: LokiStreamEncoding::Folded,
            expected: folded.clone(),
        },
        StreamEncodingCase {
            encoding: LokiStreamEncoding::CategorizeLabels,
            expected: categorized.clone(),
        },
    ] {
        let scan = HotTailScan {
            plan: &plan,
            records: &records,
            frontier: &frontier,
        };
        scan.assert_streams(case.encoding, &case.expected).await;
        assert!(scan.tail(case.encoding, TailMode::Backfill) == json!({"streams":case.expected}));
        let live = scan.tail(case.encoding, TailMode::Live);
        let expected_live = if case.encoding == LokiStreamEncoding::Folded {
            json!([
                {"stream":{"app":"api","method":"GET"},"values":[["10","line-10"],["20","line-20"]]},
                {"stream":{"app":"web","method":"GET"},"values":[["30","line-30"]]}
            ])
        } else {
            categorized.clone()
        };
        assert!(live == json!({"streams":expected_live}));
    }
    plan.query = krabka_logql::parse_query(
        r#"{app=~"api|web"} | label_format app="same" | distinct method"#,
    )
    .unwrap();
    for case in [
        StreamEncodingCase {
            encoding: LokiStreamEncoding::Folded,
            expected: json!([
                {"stream":{"app":"same","method":"GET","status":"200"},"values":[["10","line-10"],["30","line-30"]]},
                {"stream":{"app":"same","method":"GET","status":"500"},"values":[]}
            ]),
        },
        StreamEncodingCase {
            encoding: LokiStreamEncoding::CategorizeLabels,
            expected: json!([
                {"stream":{"method":"GET"},"values":[
                    ["10","line-10",{"structuredMetadata":{"status":"200"},"parsed":{"app":"same"}}],
                    ["30","line-30",{"structuredMetadata":{"status":"200"},"parsed":{"app":"same"}}]]}
            ]),
        },
    ] {
        let scan = HotTailScan {
            plan: &plan,
            records: &records,
            frontier: &frontier,
        };
        scan.assert_streams(case.encoding, &case.expected).await;
        for mode in [TailMode::Backfill, TailMode::Live] {
            assert!(scan.tail(case.encoding, mode) == json!({"streams":case.expected}));
        }
    }
    assert!(records == original);
}

/// The hot-tail records a query scans, with no blocks and no deletes.
struct HotTailScan<'a> {
    plan: &'a krabka_logql::StreamPlan,
    records: &'a [WalLogRecord],
    frontier: &'a CompactionFrontier,
}

/// Which part of a tail the scan answers.
#[derive(Clone, Copy)]
enum TailMode {
    /// The records a tail sends when it connects.
    Backfill,
    /// The records a connected tail streams.
    Live,
}

impl HotTailScan<'_> {
    async fn assert_streams(&self, encoding: LokiStreamEncoding, expected: &serde_json::Value) {
        assert!(
            self.streams(encoding).await
                == json!({"status":"success","data":{"resultType":"streams","result":expected}})
        );
    }

    async fn streams(&self, encoding: LokiStreamEncoding) -> serde_json::Value {
        execute_stream_query_with_hot_tail_frontier_and_deletes(
            ".",
            self.plan,
            &LabelIndex::default(),
            self.records,
            self.frontier,
            &[],
            encoding,
        )
        .await
        .unwrap()
    }

    fn tail(&self, encoding: LokiStreamEncoding, mode: TailMode) -> serde_json::Value {
        execute_tail_query_with_frontier_and_deletes(
            self.plan,
            self.records,
            self.frontier,
            &[],
            encoding,
            matches!(mode, TailMode::Live),
        )
    }
}
