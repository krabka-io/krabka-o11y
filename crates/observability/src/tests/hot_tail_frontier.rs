use krabka_units::convert::TimeExt as _;

use crate::tests::prelude::{
    Arc, BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockKey, BufferedLogHotTail,
    CompactionFrontier, InMemoryWalSink, LabelIndex, Labels, LogCompactionIndexOutput, LogHotTail,
    LogWalSink, ObjectPath, ObjectStore, Offset, PartitionIndex, QuerierState,
    RecordingObjectStore, ScalarVectorExpressionResult, SharedCompactionFrontier, TimeRange,
    WalLogRecord, WalPosition, check, json,
    loki_instant_scalar_or_vector_response, loki_json_timestamp_value_parse_error, minutes,
    parse_detected_labels_params, read_tenant_log_index_shard_ranges_from_object_store,
    refresh_compaction_frontier_and_prune, write_compaction_frontier_to_object_store,
    write_tenant_compaction_indexes_to_object_store,
};

/// Brute-force oracle: the records a full linear scan keeps for an inclusive window.
pub(crate) fn brute_force_in_range(
    records: &[WalLogRecord],
    start_ns: i64,
    end_ns: i64,
) -> Vec<WalLogRecord> {
    records
        .iter()
        .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
        .cloned()
        .collect()
}

pub(crate) fn hot_tail_test_record(timestamp_ns: i64, app: &str) -> WalLogRecord {
    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), app.to_string());
    WalLogRecord {
        tenant: "tenant".to_string(),
        labels,
        timestamp_ns,
        line: format!("line@{timestamp_ns}"),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

// --- FIX B1 tests ---

mod a_cancelled_frontier_refresher_joins_cleanly_through_its_handle {
use assert2::assert;
use krabka_units::millis;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::compactor::runtime::spawn_compaction_frontier_refresher;

/// Shutdown is not a failure.
///
/// A supervisor that treats every exit of this task as fatal would fail the
/// process on every clean stop, so the handle has to distinguish the two: a
/// cancelled refresher returns, and its handle joins with `Ok`.
#[tokio::test]
async fn a_cancelled_frontier_refresher_joins_cleanly_through_its_handle() {
    let token = CancellationToken::new();
    let handle = spawn_compaction_frontier_refresher(
        Arc::new(object_store::memory::InMemory::new()),
        ObjectPath::default(),
        SharedCompactionFrontier::default(),
        BufferedLogHotTail::default(),
        token.clone(),
        millis(50),
    );

    assert!(!handle.is_finished());
    token.cancel();

    let joined = tokio::time::timeout(millis(2_000).to_std(), handle)
        .await
        .expect("a cancelled refresher returns, so its handle resolves");
    assert!(let Ok(()) = joined);
}
}
mod appending_log_index_shard_does_not_rewrite_historical_shards_or_full_manifest;
mod compaction_frontier_refresh_prunes_hot_tail_from_object_store;
mod shared_snapshots_survive_pruning_and_reach_http_queries;
mod the_frontier_refresher_hands_back_a_handle_that_carries_its_panic {
use assert2::assert;
use krabka_units::millis;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::compactor::runtime::spawn_compaction_frontier_refresher;

/// An object store that dies when the frontier is read.
///
/// A refresh that returns an error is not the case under test: the loop logs
/// that and carries on. What ends the task without a word is a panic, and a
/// store that panics on `get` is the shortest way to one.
#[derive(Debug)]
struct PanicOnGetStore {
    inner: object_store::memory::InMemory,
}

impl std::fmt::Display for PanicOnGetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PanicOnGetStore")
    }
}

krabka_blockstore::delegate_object_store! {
    PanicOnGetStore => inner;
    forward [put_opts, put_multipart_opts, delete_stream, list, list_with_delimiter, copy_opts];

    async fn get_opts(
        &self,
        _location: &object_store::path::Path,
        _options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        panic!("the frontier object could not be read");
    }
}

/// The refresher's death reaches its caller.
///
/// The task honours cancellation, so shutdown always looked fine, and that is
/// what hid this: a panic inside the loop ended the refresher, the querier
/// carried on answering from the frontier it last read, and nothing anywhere
/// said the frontier had stopped moving. The handle is what makes that
/// observable, so the function has to give it back and the caller has to be
/// able to join it.
#[tokio::test]
async fn the_frontier_refresher_hands_back_a_handle_that_carries_its_panic() {
    let token = CancellationToken::new();
    let handle = spawn_compaction_frontier_refresher(
        Arc::new(PanicOnGetStore {
            inner: object_store::memory::InMemory::new(),
        }),
        ObjectPath::default(),
        SharedCompactionFrontier::default(),
        BufferedLogHotTail::default(),
        token.clone(),
        millis(1),
    );

    let joined = tokio::time::timeout(millis(2_000).to_std(), handle)
        .await
        .expect("a panicking refresher ends, so its handle resolves");

    let error = joined.expect_err("the refresher panicked, so the join must not succeed");
    assert!(error.is_panic());
    // Cancellation was never asked for: the task ended on its own, which is
    // exactly what a supervisor must be able to tell apart from shutdown.
    assert!(!token.is_cancelled());
}
}
mod the_shard_catalog_lists_each_compacted_range_exactly_once;

mod query_source_labels;

async fn absent_dynamic_index_is_an_empty_block_index(use_shards: bool) {
    use object_store::memory::InMemory;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = ObjectPath::default();
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default());
    let state = if use_shards {
        state.with_dynamic_tenant_object_store_shards(store, prefix)
    } else {
        state.with_dynamic_tenant_object_store_manifest(store, prefix)
    };
    let query_range = TimeRange::new(0, 1).unwrap();
    let result = state
        .with_request_tenant_index("test-tenant", query_range)
        .await;
    assert!(
        result.is_ok(),
        "expected Ok for absent cold index, got: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().block_index.blocks().len(), 0);
}

#[test]
pub(crate) fn detected_labels_empty_query_is_match_all() {
    // Grafana's Logs Drilldown loads `detected_labels?query=` with an empty
    // query to discover every label. An empty/blank query must parse to
    // `None` (match all streams), not be handed to the LogQL parser — which
    // rejects "" with `syntax error: unexpected $end, expecting '{'`.
    for raw in ["query=", "query=%20", "query=%20%20"] {
        let params = parse_detected_labels_params(Some(raw)).unwrap();
        assert!(params.query.is_none(), "{raw}: {:?}", params.query);
    }
    // A real stream selector is still preserved.
    let params = parse_detected_labels_params(Some("query=%7Bapp%3D%22api%22%7D")).unwrap();
    assert_eq!(params.query.as_deref(), Some(r#"{app="api"}"#));
}

#[test]
pub(crate) fn formats_loki_array_json_timestamp_error_context() {
    let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[["1000000000"],"array push timestamp"]]}]}"#;
    let timestamp = json!(["1000000000"]);
    let line = json!("array push timestamp");

    assert_eq!(
        loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}|...\n"
    );
}

#[test]
pub(crate) fn formats_loki_numeric_json_timestamp_error_context() {
    let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[1000000000,"non-string push timestamp"]]}]}"#;
    let timestamp = json!(1_000_000_000);
    let line = json!("non-string push timestamp");

    assert_eq!(
        loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|alues\":[[1000000000,\"non-string push timestamp\"]]}]}|...\n"
    );
}

#[test]
pub(crate) fn formats_loki_object_json_timestamp_error_context() {
    let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[{"ts":"1000000000"},"object push timestamp"]]}]}"#;
    let timestamp = json!({"ts": "1000000000"});
    let line = json!("object push timestamp");

    assert_eq!(
        loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|\":[[{\"ts\":\"1000000000\"},\"object push timestamp\"]]}]}|...\n"
    );
}

#[test]
pub(crate) fn hot_tail_prune_compacted_rebuilds_records_and_time_index() {
    let bucket = minutes(1).nanos_i64();

    let hot_tail = BufferedLogHotTail::default();
    let mut compacted_by_offset = hot_tail_test_record(4 * bucket, "offset-old");
    compacted_by_offset.position = Some(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(7),
    });
    let mut kept_by_offset = hot_tail_test_record(3 * bucket, "offset-new");
    kept_by_offset.position = Some(WalPosition {
        partition: PartitionIndex(0),
        offset: Offset(8),
    });
    let compacted_by_time = hot_tail_test_record(2 * bucket, "time-old");
    let kept_by_time = hot_tail_test_record(5 * bucket, "time-new");
    let expected = vec![kept_by_offset.clone(), kept_by_time.clone()];

    hot_tail.append_records(vec![
        compacted_by_offset,
        kept_by_offset,
        compacted_by_time,
        kept_by_time,
    ]);

    let frontier =
        CompactionFrontier::new(2 * bucket).with_partition_offset(PartitionIndex(0), Offset(7));

    assert_eq!(hot_tail.prune_compacted(&frontier), 2);
    assert_eq!(hot_tail.records(), expected);
    assert2::assert!(hot_tail.records_in_range(0, 6 * bucket) == expected);
    assert_eq!(hot_tail.records_in_range(2 * bucket, 2 * bucket).len(), 0);
    assert_eq!(hot_tail.records_in_range(4 * bucket, 4 * bucket).len(), 0);
}

#[test]
pub(crate) fn instant_scalar_expression_keeps_loki_seconds_timestamp() {
    let response = loki_instant_scalar_or_vector_response(
        4_000_000_000,
        ScalarVectorExpressionResult::Scalar {
            sample: "2".to_string(),
        },
    );

    assert_eq!(response["data"]["result"][0], json!(4));
}

/// A synthetic vector sample carries its timestamp in seconds, as Loki writes
/// every instant sample. Loki 3.5.1 answers `vector(1)` at
/// `time=4000000000000000000` with `[4000000000, "1"]`.
#[test]
pub(crate) fn instant_synthetic_vector_writes_loki_seconds_timestamp() {
    let response = loki_instant_scalar_or_vector_response(
        4_000_000_000_000_000_000,
        ScalarVectorExpressionResult::Vector {
            sample: Some("1".to_string()),
            metric: BTreeMap::new(),
        },
    );

    check!(response["data"]["result"][0]["value"][0] == json!(4_000_000_000_i64));
}

#[tokio::test]
pub(crate) async fn compaction_frontier_refresh_treats_absent_manifest_as_empty() {
    let store = object_store::memory::InMemory::new();
    let prefix = ObjectPath::default();
    let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(123));
    let hot_tail = BufferedLogHotTail::default();
    let fresh = hot_tail_test_record(3_000, "new");
    hot_tail.append_records(vec![fresh.clone()]);

    let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
        .await
        .unwrap();

    assert_eq!(pruned, 0);
    assert_eq!(frontier.snapshot(), CompactionFrontier::new(123));
    assert_eq!(hot_tail.records(), vec![fresh]);
}

struct TimeWindowProbe {
    start: i64,
    end: i64,
}

/// The time-bucketed `records_in_range` MUST return exactly the same records (and
/// so the same label/field sets) as a full-buffer scan, for any inclusive
/// `[start, end]`, even though records are appended in NO timestamp order. This is the
/// soundness guarantee that lets the query paths prune to the window instead of
/// scanning the whole retained buffer.
#[tokio::test]
pub(crate) async fn hot_tail_records_in_range_matches_full_scan_under_out_of_order_inserts() {
    let bucket = minutes(1).nanos_i64();

    let hot_tail = BufferedLogHotTail::default();

    // Timestamps deliberately out of order and spread across many one-minute buckets,
    // with duplicates at the same instant and records straddling bucket boundaries.
    let timestamps = [
        5 * bucket + 10,
        bucket - 1, // last ns of bucket 0
        3 * bucket,
        bucket,          // first ns of bucket 1
        5 * bucket + 10, // duplicate timestamp
        0,
        7 * bucket + 42,
        2 * bucket + 999,
        3 * bucket, // duplicate timestamp in a different append position
        4 * bucket - 1,
        -bucket + 5, // a pre-epoch (negative) timestamp
        6 * bucket,
    ];
    let apps = ["api", "web", "db"];
    let records: Vec<WalLogRecord> = timestamps
        .iter()
        .enumerate()
        .map(|entry| hot_tail_test_record(*entry.1, apps[entry.0 % apps.len()]))
        .collect();

    // Append one at a time to exercise incremental bucket insertion of out-of-order data.
    for record in &records {
        hot_tail.append_records(vec![record.clone()]);
    }

    // `records()` must still return the full append-ordered buffer (the tail path
    // depends on this).
    assert_eq!(hot_tail.records(), records);

    // Probe a wide set of windows: exact bucket edges, sub-bucket slivers, windows
    // spanning many buckets, empty windows, and windows entirely outside the data.
    let min_ts = *timestamps.iter().min().unwrap();
    let max_ts = *timestamps.iter().max().unwrap();
    let mut probes: Vec<TimeWindowProbe> = Vec::new();
    // Walk window starts at a coarse quarter-bucket stride from below the earliest
    // record to above the latest, pairing each with several spans.
    let stride = bucket / 4;
    let mut start = min_ts - 2 * bucket;
    while start <= max_ts + 2 * bucket {
        for span in [0_i64, 1, bucket - 1, bucket, bucket + 1, 3 * bucket] {
            probes.push(TimeWindowProbe {
                start,
                end: start + span,
            });
        }
        start += stride;
    }
    // Add exact per-record point windows and tight windows around each timestamp.
    for &ts in &timestamps {
        probes.push(TimeWindowProbe { start: ts, end: ts });
        probes.push(TimeWindowProbe {
            start: ts - 1,
            end: ts,
        });
        probes.push(TimeWindowProbe {
            start: ts,
            end: ts + 1,
        });
        probes.push(TimeWindowProbe {
            start: ts + 1,
            end: ts + 1,
        });
    }

    for probe in probes {
        let start = probe.start;
        let end = probe.end;
        if start > end {
            // Mirror the guard: an inverted window yields nothing.
            assert_eq!(hot_tail.records_in_range(start, end).len(), 0);
            continue;
        }
        let expected = brute_force_in_range(&records, start, end);
        let actual = hot_tail.records_in_range(start, end);
        assert_eq!(
            actual, expected,
            "records_in_range({start}, {end}) diverged from full-scan oracle"
        );

        // The label sets a query would derive must be identical too (records are the
        // sole input to label/field extraction).
        let expected_labels: BTreeSet<Labels> = expected.iter().map(|r| r.labels.clone()).collect();
        let actual_labels: BTreeSet<Labels> = actual.iter().map(|r| r.labels.clone()).collect();
        assert_eq!(
            actual_labels, expected_labels,
            "label sets diverged at [{start}, {end}]"
        );
    }

    // The trait-object path the querier actually uses must agree with the inherent method.
    let dyn_tail: Arc<dyn LogHotTail> = Arc::new(hot_tail.clone());
    let window = TimeWindowProbe {
        start: 2 * bucket,
        end: 6 * bucket,
    };
    assert_eq!(
        dyn_tail.records_in_range(window.start, window.end),
        hot_tail.records_in_range(window.start, window.end),
    );

    // The default trait impl (used by other LogHotTail implementors, e.g. the
    // in-memory sink) falls back to filtering the full buffer and must also agree.
    let in_memory = InMemoryWalSink::default();
    for record in &records {
        LogWalSink::append(&in_memory, record.clone())
            .await
            .unwrap();
    }
    let in_memory_dyn: Arc<dyn LogHotTail> = Arc::new(in_memory);
    assert_eq!(
        in_memory_dyn.records_in_range(window.start, window.end),
        brute_force_in_range(&records, window.start, window.end),
    );
}

#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_tolerates_absent_manifest() {
    crate::tests::hot_tail_frontier::absent_dynamic_index_is_an_empty_block_index(false).await;
}

#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_tolerates_absent_shards() {
    crate::tests::hot_tail_frontier::absent_dynamic_index_is_an_empty_block_index(true).await;
}
