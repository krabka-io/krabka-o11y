use assert2::assert;

use super::*;
use crate::{
    LokiStreamEncoding, execute_stream_query_with_hot_tail_frontier_and_deletes,
    execute_tail_query_with_frontier_and_deletes,
};

#[tokio::test]
async fn source_labels_preserve_distinct_and_both_tail_encodings() {
    let records = [("api", 10, "200"), ("api", 20, "500"), ("web", 30, "200")].map(
        |(app, timestamp_ns, status)| WalLogRecord {
            tenant: "tenant".into(),
            labels: Labels::from([("app".into(), app.into()), ("method".into(), "GET".into())]),
            timestamp_ns,
            line: format!("line-{timestamp_ns}"),
            structured_metadata: Labels::from([("status".into(), status.into())]),
            position: None,
        },
    );
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
        {"stream":{"app":"api","method":"GET","detected_level":"unknown","status":"200"},"values":[["10","line-10"]]},
        {"stream":{"app":"api","method":"GET","detected_level":"unknown","status":"500"},"values":[["20","line-20"]]},
        {"stream":{"app":"web","method":"GET","detected_level":"unknown","status":"200"},"values":[["30","line-30"]]}
    ]);
    let categorized = json!([
        {"stream":{"app":"api","method":"GET"},"values":[
            ["10","line-10",{"structuredMetadata":{"detected_level":"unknown","status":"200"}}],
            ["20","line-20",{"structuredMetadata":{"detected_level":"unknown","status":"500"}}]]},
        {"stream":{"app":"web","method":"GET"},"values":[
            ["30","line-30",{"structuredMetadata":{"detected_level":"unknown","status":"200"}}]]}
    ]);
    for (encoding, expected) in [
        (LokiStreamEncoding::Folded, folded.clone()),
        (LokiStreamEncoding::CategorizeLabels, categorized.clone()),
    ] {
        let actual = execute_stream_query_with_hot_tail_frontier_and_deletes(
            ".",
            &plan,
            &LabelIndex::default(),
            &records,
            &frontier,
            &[],
            encoding,
        )
        .await
        .unwrap();
        assert!(
            actual == json!({"status":"success","data":{"resultType":"streams","result":expected}})
        );
        let backfill = execute_tail_query_with_frontier_and_deletes(
            &plan,
            &records,
            &frontier,
            &[],
            encoding,
            false,
        );
        assert!(backfill == json!({"streams":expected}));
        let live = execute_tail_query_with_frontier_and_deletes(
            &plan,
            &records,
            &frontier,
            &[],
            encoding,
            true,
        );
        let expected_live = if encoding == LokiStreamEncoding::Folded {
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
    for (encoding, expected) in [
        (
            LokiStreamEncoding::Folded,
            json!([
                {"stream":{"app":"same","method":"GET","detected_level":"unknown","status":"200"},"values":[["10","line-10"],["30","line-30"]]},
                {"stream":{"app":"same","method":"GET","detected_level":"unknown","status":"500"},"values":[]}
            ]),
        ),
        (
            LokiStreamEncoding::CategorizeLabels,
            json!([
                {"stream":{"method":"GET"},"values":[
                    ["10","line-10",{"structuredMetadata":{"detected_level":"unknown","status":"200"},"parsed":{"app":"same"}}],
                    ["30","line-30",{"structuredMetadata":{"detected_level":"unknown","status":"200"},"parsed":{"app":"same"}}]]}
            ]),
        ),
    ] {
        let actual = execute_stream_query_with_hot_tail_frontier_and_deletes(
            ".",
            &plan,
            &LabelIndex::default(),
            &records,
            &frontier,
            &[],
            encoding,
        )
        .await
        .unwrap();
        assert!(
            actual == json!({"status":"success","data":{"resultType":"streams","result":expected}})
        );
        for live in [false, true] {
            let actual = execute_tail_query_with_frontier_and_deletes(
                &plan,
                &records,
                &frontier,
                &[],
                encoding,
                live,
            );
            assert!(actual == json!({"streams":expected}));
        }
    }
    assert!(records == original);
}
