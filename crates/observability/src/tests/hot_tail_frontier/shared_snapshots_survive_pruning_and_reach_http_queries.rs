use assert2::assert;

use super::*;
use crate::{
    HttpStreamQuery, LokiDirection, LokiStreamEncoding, LokiStreamOptions,
    execute_http_stream_query, execute_stream_query_with_hot_tail_frontier_and_deletes,
    hot_tail_snapshot,
};

struct SharedOnly(BufferedLogHotTail);

impl LogHotTail for SharedOnly {
    fn records(&self) -> Vec<WalLogRecord> {
        panic!("the HTTP query must use shared records")
    }

    fn records_shared_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<Arc<WalLogRecord>> {
        self.0.records_shared_in_range(start_ns, end_ns)
    }
}

fn expected_response(result: &serde_json::Value, lines: u64) -> serde_json::Value {
    let mut stats = crate::loki_query_stats();
    stats["ingester"]["decompressedLines"] = json!(lines);
    stats["ingester"]["totalLinesSent"] = json!(lines);
    stats["summary"]["totalLinesProcessed"] = json!(lines);
    json!({
        "status": "success", "data": {"resultType": "streams", "result": result,
        "stats": stats
    }})
}

#[tokio::test]
async fn shared_snapshots_survive_pruning_and_reach_http_queries() {
    let mut first = hot_tail_test_record(100, "api");
    first.line = "first".into();
    first.position = Some(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(1),
    });
    first
        .structured_metadata
        .insert("request".into(), "a".into());
    let mut second = hot_tail_test_record(120, "api");
    second.line = "second".into();
    second.position = Some(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(2),
    });
    second
        .structured_metadata
        .insert("request".into(), "b".into());
    let mut other_tenant = hot_tail_test_record(110, "api");
    other_tenant.tenant = "other".into();
    let ledger = vec![
        second.clone(),
        first.clone(),
        first,
        hot_tail_test_record(80, "api"),
        hot_tail_test_record(90, "web"),
        other_tenant,
        hot_tail_test_record(79, "api"),
        hot_tail_test_record(121, "api"),
    ];
    let tail = BufferedLogHotTail::default();
    tail.append_records(ledger.clone());
    let dynamic: Arc<dyn LogHotTail> = Arc::new(tail.clone());
    let frontier = SharedCompactionFrontier::default();
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_hot_tail_shared_frontier(SharedOnly(tail.clone()), frontier.clone());
    let range = TimeRange::new(80, 120).unwrap();
    let snapshot = hot_tail_snapshot(&state, range);
    let captured = &snapshot.records;
    let captured_frontier = &snapshot.frontier;
    let expected_records = ledger[..6].to_vec();
    assert!(
        captured.iter().map(Arc::as_ref).collect::<Vec<_>>()
            == expected_records.iter().collect::<Vec<_>>()
    );
    let shared = dynamic.records_shared_in_range(80, 120);
    assert!(
        shared
            .iter()
            .zip(captured.iter())
            .all(|(a, b)| Arc::ptr_eq(a, b))
    );
    assert!(shared.len() == captured.len());
    drop(shared);
    let weak = Arc::downgrade(&captured[1]);
    struct RecordRangeCase {
        start: i64,
        end: i64,
    }
    for range in [
        RecordRangeCase {
            start: 80,
            end: 120,
        },
        RecordRangeCase {
            start: 100,
            end: 100,
        },
        RecordRangeCase { start: -1, end: 80 },
        RecordRangeCase {
            start: 121,
            end: 120,
        },
    ] {
        let expected = ledger
            .iter()
            .filter(|r| r.timestamp_ns >= range.start && r.timestamp_ns <= range.end)
            .collect::<Vec<_>>();
        let actual = dynamic.records_shared_in_range(range.start, range.end);
        assert!(actual.iter().map(Arc::as_ref).collect::<Vec<_>>() == expected);
    }
    let sink = InMemoryWalSink::default();
    for record in &ledger {
        LogWalSink::append(&sink, record.clone()).await.unwrap();
    }
    let fallback: Arc<dyn LogHotTail> = Arc::new(sink);
    assert!(
        fallback
            .records_shared_in_range(80, 120)
            .iter()
            .map(Arc::as_ref)
            .collect::<Vec<_>>()
            == expected_records.iter().collect::<Vec<_>>()
    );
    let result = json!([
        {"stream": {"app": "api"}, "values": [["80", "line@80"]]},
        {"stream": {"app": "api", "request": "a"},
            "values": [["100", "first"], ["100", "first"]]},
        {"stream": {"app": "api", "request": "b"},
            "values": [["120", "second"]]}
    ]);
    let expected = expected_response(&result, 4);
    let stream_query = HttpStreamQuery {
        query: "{app=\"api\"}",
        tenant: "tenant",
        time_range: range,
        options: LokiStreamOptions {
            direction: LokiDirection::Forward,
            limit: None,
            interval: None,
        },
        end_exclusive: None,
        encoding: LokiStreamEncoding::Folded,
    };
    for object_store in [false, true] {
        let state = if object_store {
            state.clone().with_cold_object_store_source(
                Arc::new(object_store::memory::InMemory::new()),
                ObjectPath::default(),
            )
        } else {
            state.clone()
        };
        let actual = execute_http_stream_query(&state, stream_query)
            .await
            .unwrap();
        assert!(actual == expected);
    }
    let next_frontier =
        CompactionFrontier::new(80).with_partition_offset(PartitionIndex(0), Offset(1));
    frontier.replace(next_frontier.clone());
    assert!(tail.prune_compacted(&next_frontier) == 4);
    tail.append_records(vec![hot_tail_test_record(90, "api")]);
    let plan = krabka_logql::StreamPlan {
        tenant: "tenant".into(),
        time_range: range,
        query: krabka_logql::parse_query("{app=\"api\"}").unwrap(),
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let held = execute_stream_query_with_hot_tail_frontier_and_deletes(
        ".",
        &plan,
        &LabelIndex::default(),
        &captured,
        &captured_frontier,
        &[],
        LokiStreamEncoding::Folded,
    )
    .await
    .unwrap();
    assert!(
        held == json!({"status": "success", "data": {"resultType": "streams", "result": result}})
    );
    assert!(
        captured.iter().map(Arc::as_ref).collect::<Vec<_>>()
            == expected_records.iter().collect::<Vec<_>>()
    );
    let fresh_result = json!([
        {"stream": {"app": "api"}, "values": [["90", "line@90"]]},
        {"stream": {"app": "api", "request": "b"},
            "values": [["120", "second"]]}
    ]);
    let fresh = execute_http_stream_query(&state, stream_query)
        .await
        .unwrap();
    assert!(fresh == expected_response(&fresh_result, 2));
    drop(captured);
    assert!(weak.upgrade().is_none());
}
