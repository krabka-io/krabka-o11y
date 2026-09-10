use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{Array, DictionaryArray, FixedSizeBinaryArray, Int64Array, StringArray},
    datatypes::Int32Type,
};
use assert2::check;
use bytes::Bytes;
use futures::stream::BoxStream;
use krabka_blockstore::{
    BlockLevel, BlockWriter, PromotedSpanAttr, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom,
    TraceBlockStats, TraceIndex, read_block,
};
use krabka_client_consumer::ConsumerRecord;
use krabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode, TracesError,
    blockbuilder::{
        BlockBuilderConfig, WalConsumerCommit, WalConsumerPoll, build_blocks,
        build_blocks_with_prefix, build_blocks_with_promoted_attrs, decode_consumer_records,
        flush_partition_windows, group_by_trace, object_key, run,
    },
    ids::{MaxOffset, MinOffset, WindowStartNs},
    metrics::ServiceMetrics,
};
use krabka_units::{hours, millis, minutes};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tokio::sync::{Mutex, oneshot};
use tokio_util::sync::CancellationToken;

fn span(trace_id: [u8; 16], span_id: u8, parent: Option<u8>, start_ns: i64) -> Span {
    Span {
        trace_id,
        span_id: [span_id; 8],
        parent_span_id: parent.map(|id| [id; 8]),
        name: format!("span-{span_id}"),
        kind: SpanKind::Server,
        start_ns,
        duration_ns: 5,
        status: StatusCode::Ok,
        status_message: String::new(),
        resource_attrs: vec![KeyValue {
            key: "service.name".into(),
            value: AttrValue::Str("api".into()),
        }],
        span_attrs: vec![KeyValue {
            key: "http.method".into(),
            value: AttrValue::Str("GET".into()),
        }],
        events: Vec::new(),
        links: Vec::new(),
        instrumentation_scope: "test".into(),
        instrumentation_version: String::new(),
    }
}

fn rec(
    tenant: &str,
    trace_id: [u8; 16],
    span_id: u8,
    parent: Option<u8>,
    start_ns: i64,
) -> SpanRecord {
    SpanRecord {
        tenant: tenant.into(),
        span: span(trace_id, span_id, parent, start_ns),
    }
}

fn consumer_record(partition: i32, offset: i64, record: &SpanRecord) -> ConsumerRecord {
    ConsumerRecord {
        topic: "__krabka_traces_wal".into(),
        partition,
        offset,
        leader_epoch: 0,
        timestamp: 0,
        key: None,
        value: Some(Bytes::from(record.encode().unwrap())),
        headers: Vec::new(),
    }
}

#[test]
fn object_key_is_deterministic_and_offset_scoped() {
    let a = object_key(
        "tenant-a",
        3,
        MinOffset(10),
        MaxOffset(20),
        WindowStartNs(1_000),
    );
    let b = object_key(
        "tenant-a",
        3,
        MinOffset(10),
        MaxOffset(20),
        WindowStartNs(1_000),
    );
    let c = object_key(
        "tenant-a",
        3,
        MinOffset(10),
        MaxOffset(21),
        WindowStartNs(1_000),
    );

    check!(a == b);
    check!(a != c);
    check!(a == "traces/tenant-a/00003/00000000000000000010-00000000000000000020-1000.parquet");
}

#[test]
fn group_by_trace_orders_spans_per_tenant_trace() {
    let records = vec![
        rec("tenant-a", [1; 16], 2, Some(1), 200),
        rec("tenant-b", [1; 16], 9, None, 50),
        rec("tenant-a", [1; 16], 1, None, 100),
    ];

    let grouped = group_by_trace(&records);
    let group = &grouped[&("tenant-a".to_string(), [1; 16])];

    assert2::assert!(
        group.iter().map(|span| span.span_id).collect::<Vec<_>>() == vec![[1; 8], [2; 8]]
    );
    assert2::assert!(grouped[&("tenant-b".to_string(), [1; 16])][0].span_id == [9; 8]);
}

#[test]
fn decode_consumer_records_groups_by_partition_and_tracks_offsets() {
    let windows = decode_consumer_records(&[
        consumer_record(1, 11, &rec("tenant-a", [1; 16], 1, None, 100)),
        consumer_record(1, 12, &rec("tenant-a", [1; 16], 2, Some(1), 200)),
        consumer_record(2, 7, &rec("tenant-b", [2; 16], 1, None, 50)),
        ConsumerRecord {
            topic: "__krabka_traces_wal".into(),
            partition: 1,
            offset: 13,
            leader_epoch: 0,
            timestamp: 0,
            key: None,
            value: None,
            headers: Vec::new(),
        },
    ])
    .unwrap();

    check!(
        windows
            .iter()
            .map(|(partition, window)| {
                (
                    *partition,
                    window.offset_range,
                    window.records.len(),
                    window.records[0].tenant.as_str(),
                )
            })
            .collect::<Vec<_>>()
            == vec![(1, (11, 12), 2, "tenant-a"), (2, (7, 7), 1, "tenant-b")]
    );
}

#[tokio::test]
async fn build_blocks_writes_span_block_and_updates_trace_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![
        rec("tenant-a", [1; 16], 2, Some(1), 200),
        rec("tenant-a", [1; 16], 1, None, 100),
        rec("tenant-b", [2; 16], 1, None, 50),
    ];

    let metas = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();

    check!(
        metas
            .iter()
            .map(|meta| (
                meta.tenant.as_str(),
                meta.row_count,
                meta.min_ts,
                meta.max_ts
            ))
            .collect::<Vec<_>>()
            == vec![("tenant-a", 2, 100, 200)]
    );

    let batches = read_block(store, &metas[0].object_key).await.unwrap();
    check!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![metas[0].object_key.clone()]
    );
    check!(
        index.prune_blocks_by_tag("tenant-a", "service.name", Some("api"), 0, 1_000)
            == vec![metas[0].object_key.clone()]
    );
    check!(
        index
            .candidate_blocks_for_trace("tenant-b", &[2; 16], 0, 1_000)
            .is_empty()
    );
}

#[tokio::test]
async fn replaying_same_offset_window_is_idempotent_in_trace_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![
        rec("tenant-a", [1; 16], 2, Some(1), 200),
        rec("tenant-a", [1; 16], 1, None, 100),
    ];

    let first = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();
    let replay = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();

    check!(first[0].object_key == replay[0].object_key);
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![first[0].object_key.clone()]
    );
    check!(
        index.prune_blocks_by_tag("tenant-a", "service.name", Some("api"), 0, 1_000)
            == vec![first[0].object_key.clone()]
    );
    let batches = read_block(store, &first[0].object_key).await.unwrap();
    assert2::assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
}

#[tokio::test]
async fn replaying_saved_partition_window_after_restart_is_idempotent() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let config = krabka_traces::blockbuilder::BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: millis(1),
        empty_poll_backoff: millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: krabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS,
        flush_max_age: krabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_AGE,
        index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
    };
    let records = [
        consumer_record(7, 10, &rec("tenant-a", [1; 16], 2, Some(1), 200)),
        consumer_record(7, 11, &rec("tenant-a", [1; 16], 1, None, 100)),
    ];
    let windows = decode_consumer_records(&records).unwrap();
    let mut index = TraceIndex::new();

    flush_partition_windows(&writer, &mut index, store.clone(), &config, windows.clone())
        .await
        .unwrap();
    let mut restarted = TraceIndex::load_latest_snapshot(&store, "index/traces.json")
        .await
        .unwrap();

    flush_partition_windows(&writer, &mut restarted, store.clone(), &config, windows)
        .await
        .unwrap();
    let reloaded = TraceIndex::load_latest_snapshot(&store, "index/traces.json")
        .await
        .unwrap();

    assert2::assert!(
        store
            .head(&object_store::path::Path::from("index/traces.json"))
            .await
            .is_err()
    );

    assert2::assert!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![
                "traces/tenant-a/00007/00000000000000000010-00000000000000000011-100.parquet"
                    .to_string()
            ]
    );
}

#[tokio::test]
async fn configured_index_snapshot_retention_is_applied() {
    use std::collections::BTreeMap;

    use futures::StreamExt as _;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let mut config = block_builder_config();
    config.index_snapshot_retain = krabka_blockstore::IndexSnapshotRetain::new(2).unwrap();

    for _ in 0..4 {
        flush_partition_windows(&writer, &mut index, store.clone(), &config, BTreeMap::new())
            .await
            .unwrap();
    }

    let prefix = object_store::path::Path::from(krabka_blockstore::index_snapshot_prefix_for_key(
        &config.index_key,
    ));
    let mut snapshots = store.list(Some(&prefix));
    let mut count = 0;
    while let Some(snapshot) = snapshots.next().await {
        snapshot.unwrap();
        count += 1;
    }
    assert2::assert!(count == 2);
}

#[tokio::test]
async fn multiple_polls_below_threshold_flush_one_block_per_partition() {
    use krabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: millis(1),
        empty_poll_backoff: millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 50_000,
        flush_max_age: minutes(1),
        index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
    };

    // Three polls, each well under the flush threshold, all for the same trace
    // across two polls plus a second trace in the third poll.
    let poll1 = decode_consumer_records(&[consumer_record(
        7,
        10,
        &rec("tenant-a", [1; 16], 1, None, 100),
    )])
    .unwrap();
    let poll2 = decode_consumer_records(&[consumer_record(
        7,
        11,
        &rec("tenant-a", [1; 16], 2, Some(1), 200),
    )])
    .unwrap();
    let poll3 = decode_consumer_records(&[consumer_record(
        7,
        12,
        &rec("tenant-a", [2; 16], 3, None, 300),
    )])
    .unwrap();

    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(poll1, Instant::now());
    assert2::assert!(!accumulator.should_flush(&config, Instant::now()));
    accumulator.merge(poll2, Instant::now());
    assert2::assert!(!accumulator.should_flush(&config, Instant::now()));
    accumulator.merge(poll3, Instant::now());
    assert2::assert!(!accumulator.should_flush(&config, Instant::now()));
    assert2::assert!(accumulator.record_count() == 3);

    // A single flush of the merged buffer writes ONE block for the partition
    // (not one block per poll), covering the full offset range 10..=12.
    let windows = accumulator.take();
    assert2::assert!(accumulator.is_empty());
    let mut index = TraceIndex::new();
    flush_partition_windows(&writer, &mut index, store.clone(), &config, windows)
        .await
        .unwrap();

    let key = "traces/tenant-a/00007/00000000000000000010-00000000000000000012-100.parquet";
    let batches = read_block(store, key).await.unwrap();
    // Both spans of trace [1;16] grouped across polls + the lone span of [2;16].
    check!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 3
    );
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000) == vec![key.to_string()]
    );
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[2; 16], 0, 1_000) == vec![key.to_string()]
    );
}

#[tokio::test]
async fn accumulator_flushes_on_record_count_threshold() {
    use krabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: millis(1),
        empty_poll_backoff: millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 2,
        flush_max_age: minutes(1),
        index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
    };

    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(0, 1, &rec("t", [1; 16], 1, None, 1))]).unwrap(),
        Instant::now(),
    );
    // One record buffered, below the threshold of 2.
    assert2::assert!(!accumulator.should_flush(&config, Instant::now()));

    accumulator.merge(
        decode_consumer_records(&[consumer_record(0, 2, &rec("t", [1; 16], 2, None, 2))]).unwrap(),
        Instant::now(),
    );
    // Two records buffered -> threshold reached.
    assert2::assert!(accumulator.should_flush(&config, Instant::now()));
}

#[tokio::test]
async fn accumulator_flushes_on_age_for_low_traffic_stream() {
    use krabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: millis(1),
        empty_poll_backoff: millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 50_000,
        flush_max_age: minutes(1),
        index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
    };

    let mut accumulator = FlushAccumulator::new();
    let start = Instant::now();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(0, 1, &rec("t", [1; 16], 1, None, 1))]).unwrap(),
        start,
    );

    // Far below the record threshold, and the oldest record is young.
    assert2::assert!(
        !accumulator.should_flush(&config, start + std::time::Duration::from_secs(59))
    );
    // Once the oldest buffered record ages past flush_max_age, flush anyway so a
    // low-traffic stream stays queryable.
    assert2::assert!(accumulator.should_flush(&config, start + std::time::Duration::from_mins(1)));
}

#[tokio::test]
async fn shutdown_drain_flushes_remaining_buffer_without_losing_spans() {
    use krabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: millis(1),
        empty_poll_backoff: millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 50_000,
        flush_max_age: minutes(1),
        index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
    };

    // Two polls buffered, never reaching the flush threshold (mirrors a pending
    // buffer at shutdown).
    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(
            3,
            5,
            &rec("tenant-a", [4; 16], 1, None, 100),
        )])
        .unwrap(),
        Instant::now(),
    );
    accumulator.merge(
        decode_consumer_records(&[consumer_record(
            3,
            6,
            &rec("tenant-a", [4; 16], 2, Some(1), 200),
        )])
        .unwrap(),
        Instant::now(),
    );
    assert2::assert!(!accumulator.should_flush(&config, Instant::now()));

    // The shutdown drain path: a non-empty buffer is flushed before exit.
    assert2::assert!(!accumulator.is_empty());
    let windows = accumulator.take();
    let mut index = TraceIndex::new();
    flush_partition_windows(&writer, &mut index, store.clone(), &config, windows)
        .await
        .unwrap();

    // No spans lost: both buffered spans land in the durable block.
    let key = "traces/tenant-a/00003/00000000000000000005-00000000000000000006-100.parquet";
    let batches = read_block(store, key).await.unwrap();
    assert2::assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
}

#[tokio::test]
async fn merged_buffer_offset_range_is_stable_for_idempotent_keying() {
    use krabka_traces::blockbuilder::FlushAccumulator;
    use tokio::time::Instant;

    // Polls arriving out of order still produce a buffer whose offset range spans
    // every buffered record, so the derived block key is stable across re-runs.
    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(7, 12, &rec("t", [1; 16], 2, None, 200))])
            .unwrap(),
        Instant::now(),
    );
    accumulator.merge(
        decode_consumer_records(&[consumer_record(7, 10, &rec("t", [1; 16], 1, None, 100))])
            .unwrap(),
        Instant::now(),
    );
    accumulator.merge(
        decode_consumer_records(&[consumer_record(7, 11, &rec("t", [1; 16], 3, None, 150))])
            .unwrap(),
        Instant::now(),
    );

    let windows = accumulator.take();
    let window = &windows[&7];
    assert2::assert!((window.offset_range, window.records.len()) == ((10, 12), 3));
}

#[tokio::test]
async fn build_blocks_with_prefix_scopes_block_keys() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![rec("tenant-a", [1; 16], 1, None, 100)];

    let metas = build_blocks_with_prefix(
        &writer,
        &mut index,
        "tempo/traces",
        "tenant-a",
        7,
        &records,
        (10, 20),
    )
    .await
    .unwrap();

    check!(
        metas
            .iter()
            .map(|meta| meta.object_key.as_str())
            .collect::<Vec<_>>()
            == vec![
                "tempo/traces/traces/tenant-a/00007/00000000000000000010-00000000000000000020-100.parquet"
            ]
    );
    check!(read_block(store, &metas[0].object_key).await.is_ok());
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![metas[0].object_key.clone()]
    );
}

#[tokio::test]
async fn build_blocks_promotes_configured_attribute_columns() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![rec("tenant-a", [1; 16], 1, None, 100)];

    let metas = build_blocks_with_promoted_attrs(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &records,
        (10, 20),
        &[PromotedSpanAttr::string("http.method")],
    )
    .await
    .unwrap();

    let batches = read_block(store, &metas[0].object_key).await.unwrap();
    let methods = batches[0]
        .column_by_name("attr.http.method")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    let values = methods
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let key = usize::try_from(methods.keys().value(0)).unwrap();
    assert2::assert!(values.value(key) == "GET");
}

/// The span block declares `[trace_id, start_unix_nano]` as its sort key, and
/// the block's trace-id bounds and Parquet `sorting_columns` only describe the
/// file if its rows really are in that order. The builder produces that order
/// itself: it orders the per-trace batches by trace id, and `group_by_trace`
/// orders each trace's spans by start. Leaving it to the order the grouping
/// map happens to iterate in would make the invariant an accident of the map's
/// key type.
#[tokio::test]
async fn a_written_block_is_ordered_by_trace_id_then_start() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    // Interleaved: neither the trace ids nor the starts arrive in order.
    let records = vec![
        rec("tenant-a", [2; 16], 1, None, 300),
        rec("tenant-a", [1; 16], 2, None, 100),
        rec("tenant-a", [2; 16], 3, Some(1), 250),
        rec("tenant-a", [1; 16], 4, Some(2), 150),
    ];

    let metas = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();

    let batches = read_block(store, &metas[0].object_key).await.unwrap();
    let mut trace_ids = Vec::new();
    let mut starts = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name(SCOL_TRACE_ID)
            .expect("the trace id column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("a fixed-size binary column");
        trace_ids.extend((0..ids.len()).map(|row| ids.value(row).to_vec()));
        starts.extend(
            batch
                .column_by_name(SCOL_START_NANO)
                .expect("the start column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 column")
                .values()
                .iter()
                .copied(),
        );
    }

    check!(trace_ids == vec![vec![1_u8; 16], vec![1; 16], vec![2; 16], vec![2; 16]]);
    check!(starts == vec![100, 150, 250, 300]);
}

/// Shared, ordered event log.
///
/// Object-store `put`s and consumer `commit`s push markers here, so a test can
/// assert that the durable write strictly precedes the offset commit. That is
/// the at-least-once invariant of `flush_and_commit`.
type EventLog = Arc<StdMutex<Vec<String>>>;

/// Object store that records every `put`, that is every block or index write,
/// into a shared event log. It can also fail `put` once a flush is reached.
/// Everything else delegates to an inner [`InMemory`] store.
struct RecordingObjectStore {
    inner: Arc<InMemory>,
    events: EventLog,
    fail_puts: bool,
}

impl RecordingObjectStore {
    fn recording(events: EventLog) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            events,
            fail_puts: false,
        }
    }

    fn failing(events: EventLog) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            events,
            fail_puts: true,
        }
    }
}

impl std::fmt::Debug for RecordingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

impl std::fmt::Display for RecordingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_puts {
            return Err(object_store::Error::Generic {
                store: "RecordingObjectStore",
                source: "injected put failure".into(),
            });
        }
        self.events
            .lock()
            .expect("events lock")
            .push(format!("put:{location}"));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Scripted WAL consumer.
///
/// It returns each queued batch in turn. It then cancels the shutdown token and
/// returns empty, so `run` exits its loop and reaches the drain path. It
/// records every `commit_sync` both as a count and as an ordered marker in the
/// shared event log.
struct ScriptedConsumer {
    batches: std::collections::VecDeque<Vec<ConsumerRecord>>,
    shutdown: CancellationToken,
    commit_calls: Arc<AtomicUsize>,
    events: EventLog,
}

impl ScriptedConsumer {
    fn new(
        batches: Vec<Vec<ConsumerRecord>>,
        shutdown: CancellationToken,
        commit_calls: Arc<AtomicUsize>,
        events: EventLog,
    ) -> Self {
        Self {
            batches: batches.into(),
            shutdown,
            commit_calls,
            events,
        }
    }
}

#[async_trait::async_trait]
impl WalConsumerPoll for ScriptedConsumer {
    async fn poll(
        &mut self,
        _window: krabka_units::Time,
    ) -> Result<Vec<ConsumerRecord>, TracesError> {
        if let Some(batch) = self.batches.pop_front() {
            Ok(batch)
        } else {
            // Scripted input exhausted: stop the loop so `run` proceeds to its
            // shutdown drain on whatever is still buffered.
            self.shutdown.cancel();
            Ok(Vec::new())
        }
    }
}

#[async_trait::async_trait]
impl WalConsumerCommit for ScriptedConsumer {
    async fn commit_sync(&mut self) -> Result<(), TracesError> {
        self.commit_calls.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .expect("events lock")
            .push("commit".to_string());
        Ok(())
    }
}

fn block_builder_config() -> BlockBuilderConfig {
    BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: millis(1),
        empty_poll_backoff: millis(1),
        promoted_attrs: Vec::new(),
        // Below-threshold counts never trip the count flush; the loop drains on
        // shutdown instead, exercising the drain path the tests target.
        flush_max_records: 50_000,
        flush_max_age: hours(1),
        index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
    }
}

#[tokio::test]
async fn run_commits_offsets_only_after_a_durable_block_write() {
    let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
    let store = Arc::new(RecordingObjectStore::recording(Arc::clone(&events)));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = BlockWriter::new(object_store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let shutdown = CancellationToken::new();
    let commit_calls = Arc::new(AtomicUsize::new(0));

    // One poll of two spans for the same trace, well below the flush threshold,
    // so the only flush+commit happens on the shutdown drain.
    let batch = vec![
        consumer_record(3, 10, &rec("tenant-a", [1; 16], 1, None, 100)),
        consumer_record(3, 11, &rec("tenant-a", [1; 16], 2, Some(1), 200)),
    ];
    let consumer = ScriptedConsumer::new(
        vec![batch],
        shutdown.clone(),
        Arc::clone(&commit_calls),
        Arc::clone(&events),
    );

    run(
        consumer,
        writer,
        Arc::clone(&index),
        object_store.clone(),
        block_builder_config(),
        ServiceMetrics::new(),
        shutdown,
    )
    .await
    .unwrap();

    // Commit happened exactly once.
    assert2::assert!(commit_calls.load(Ordering::SeqCst) == 1);

    // ...and strictly AFTER the durable writes: every `put` marker precedes the
    // single `commit` marker. Reordering `flush_and_commit` to commit-before-flush
    // would put "commit" first and fail this.
    let recorded = events.lock().expect("events lock").clone();
    let commit_index = recorded.iter().position(|e| e == "commit").unwrap();
    check!(commit_index == recorded.len() - 1);
    check!(
        recorded[..commit_index]
            .iter()
            .all(|e| e.starts_with("put:"))
    );
    check!(commit_index >= 1);

    // The block is durable and the index references it: no data lost.
    let key = "traces/tenant-a/00003/00000000000000000010-00000000000000000011-100.parquet";
    let batches = read_block(object_store, key).await.unwrap();
    assert2::assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
    assert2::assert!(
        index
            .lock()
            .await
            .candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![key.to_string()]
    );
}

#[tokio::test]
async fn run_does_not_commit_when_the_flush_write_fails() {
    let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
    // Every `put` errors, so `flush_partition_windows` fails on the first block
    // (or index) write and `flush_and_commit` returns Err before commit_sync.
    let store = Arc::new(RecordingObjectStore::failing(Arc::clone(&events)));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = BlockWriter::new(object_store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let shutdown = CancellationToken::new();
    let commit_calls = Arc::new(AtomicUsize::new(0));

    let batch = vec![consumer_record(
        3,
        10,
        &rec("tenant-a", [1; 16], 1, None, 100),
    )];
    let consumer = ScriptedConsumer::new(
        vec![batch],
        shutdown.clone(),
        Arc::clone(&commit_calls),
        Arc::clone(&events),
    );

    let result = run(
        consumer,
        writer,
        Arc::clone(&index),
        object_store,
        block_builder_config(),
        ServiceMetrics::new(),
        shutdown,
    )
    .await;

    // The drain flush failed, so `run` propagates the error...
    assert2::assert!(result.is_err());
    // ...and offsets were NOT committed: they stay behind the undurable data so a
    // restart re-reads them (at-least-once). Committing on the error path would
    // make this counter non-zero.
    assert2::assert!(commit_calls.load(Ordering::SeqCst) == 0);
    let recorded = events.lock().expect("events lock").clone();
    assert2::assert!(recorded.iter().all(|e| e != "commit"));
}

#[tokio::test]
async fn run_drains_remaining_buffer_exactly_once_on_shutdown() {
    let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
    let store = Arc::new(RecordingObjectStore::recording(Arc::clone(&events)));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = BlockWriter::new(object_store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let shutdown = CancellationToken::new();
    let commit_calls = Arc::new(AtomicUsize::new(0));

    // Two below-threshold polls buffer four spans for one trace; the count never
    // trips `flush_max_records`, so nothing flushes inside the loop. Only the
    // shutdown drain should flush — exactly one block and exactly one commit.
    let consumer = ScriptedConsumer::new(
        vec![
            vec![
                consumer_record(3, 5, &rec("tenant-a", [4; 16], 1, None, 100)),
                consumer_record(3, 6, &rec("tenant-a", [4; 16], 2, Some(1), 200)),
            ],
            vec![
                consumer_record(3, 7, &rec("tenant-a", [4; 16], 3, Some(2), 300)),
                consumer_record(3, 8, &rec("tenant-a", [4; 16], 4, Some(3), 400)),
            ],
        ],
        shutdown.clone(),
        Arc::clone(&commit_calls),
        Arc::clone(&events),
    );

    run(
        consumer,
        writer,
        Arc::clone(&index),
        object_store.clone(),
        block_builder_config(),
        ServiceMetrics::new(),
        shutdown,
    )
    .await
    .unwrap();

    // Exactly one drain commit. Deleting the `if !accumulator.is_empty()` drain
    // block leaves the buffer unflushed -> zero commits -> this fails.
    assert2::assert!(commit_calls.load(Ordering::SeqCst) == 1);
    let recorded = events.lock().expect("events lock").clone();
    assert2::assert!(recorded.iter().filter(|e| *e == "commit").count() == 1);

    // No spans dropped: all four buffered spans land in the single drained block,
    // whose key spans the merged offset range 5..=8.
    let key = "traces/tenant-a/00003/00000000000000000005-00000000000000000008-100.parquet";
    let batches = read_block(object_store, key).await.unwrap();
    assert2::assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 4
    );
    assert2::assert!(
        index
            .lock()
            .await
            .candidate_blocks_for_trace("tenant-a", &[4; 16], 0, 1_000)
            == vec![key.to_string()]
    );
}

/// Object store that holds each writer's *first* trace-index manifest `put` at
/// a barrier.
///
/// Every block builder reaches the gate before any of them is allowed to write,
/// so a test creates a genuine overlap between concurrent
/// `save_latest_snapshot_*` calls without a sleep: the barrier releases only
/// once all the writers are inside their snapshot write. Only the first
/// `gated_puts` snapshot writes wait, which is exactly one per writer; a writer
/// that loses the race and retries must not block on peers that have already
/// finished. Every other operation delegates straight through to an inner
/// [`InMemory`] store.
///
/// The gate is on the snapshot prefix rather than on the index key's whole
/// subtree, because a snapshot write is now two kinds of put: the shard
/// payloads, which are content-addressed and so cannot conflict, and the
/// manifest, which is the conditional create that decides the race. Gating the
/// payloads would release the writers before either had reached the write that
/// contends.
struct IndexSnapshotBarrierStore {
    inner: Arc<InMemory>,
    snapshot_prefix: String,
    gate: Arc<tokio::sync::Barrier>,
    gated_puts: usize,
    snapshot_puts: AtomicUsize,
}

impl IndexSnapshotBarrierStore {
    fn new(snapshot_prefix: &str, gate: Arc<tokio::sync::Barrier>, gated_puts: usize) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            snapshot_prefix: snapshot_prefix.to_string(),
            gate,
            gated_puts,
            snapshot_puts: AtomicUsize::new(0),
        }
    }

    /// Snapshot writes attempted, retries included.
    ///
    /// A test asserts this exceeds the writer count. That is the evidence the
    /// barrier did its job: writers that never overlapped would each land their
    /// first write, and the total would equal the writer count exactly.
    fn snapshot_put_count(&self) -> usize {
        self.snapshot_puts.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for IndexSnapshotBarrierStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IndexSnapshotBarrierStore")
    }
}

impl std::fmt::Display for IndexSnapshotBarrierStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IndexSnapshotBarrierStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for IndexSnapshotBarrierStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with(&self.snapshot_prefix)
            && self.snapshot_puts.fetch_add(1, Ordering::SeqCst) < self.gated_puts
        {
            self.gate.wait().await;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Two block-builder replicas share one object store and one `index_key`, and
/// each owns its own WAL partition, so their block sets are disjoint. Each
/// keeps its own in-memory [`TraceIndex`], which is what `run_block_builder`
/// builds: one `load_latest_snapshot_or_empty` at startup, and an
/// `Arc<Mutex<_>>` from then on.
///
/// The barrier makes the two snapshot writes genuinely concurrent without a
/// sleep. Neither `put` proceeds until both are inside it. Afterwards the index
/// that any restarted builder or querier loads must still name both writers'
/// blocks. Both blocks are durable in the store either way, so anything missing
/// here is a block that exists but can never be found.
///
/// `save_latest_snapshot_with_retain` merges into the newest stored snapshot
/// and publishes the result under the next generation with a conditional
/// create, so the loser of the race re-reads the winner's snapshot and folds
/// its own blocks into that.
#[tokio::test]
async fn concurrent_block_builders_sharing_one_index_key_keep_both_blocks_queryable() {
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let barrier = Arc::new(IndexSnapshotBarrierStore::new(
        "index/traces/snapshots",
        gate,
        2,
    ));
    let store: Arc<dyn ObjectStore> = Arc::clone(&barrier) as Arc<dyn ObjectStore>;
    let config = block_builder_config();

    let windows_a = decode_consumer_records(&[consumer_record(
        3,
        10,
        &rec("tenant-a", [1; 16], 1, None, 100),
    )])
    .unwrap();
    let windows_b = decode_consumer_records(&[consumer_record(
        4,
        20,
        &rec("tenant-a", [2; 16], 1, None, 200),
    )])
    .unwrap();

    let builder_a = {
        let store = Arc::clone(&store);
        let config = &config;
        async move {
            let writer = BlockWriter::new(Arc::clone(&store));
            let mut index = TraceIndex::new();
            flush_partition_windows(&writer, &mut index, store, config, windows_a)
                .await
                .unwrap();
        }
    };
    let builder_b = {
        let store = Arc::clone(&store);
        let config = &config;
        async move {
            let writer = BlockWriter::new(Arc::clone(&store));
            let mut index = TraceIndex::new();
            flush_partition_windows(&writer, &mut index, store, config, windows_b)
                .await
                .unwrap();
        }
    };
    tokio::join!(builder_a, builder_b);

    let key_a = "traces/tenant-a/00003/00000000000000000010-00000000000000000010-100.parquet";
    let key_b = "traces/tenant-a/00004/00000000000000000020-00000000000000000020-200.parquet";

    // Both blocks are durably written: the loss, if any, is index-only.
    check!(read_block(Arc::clone(&store), key_a).await.is_ok());
    check!(read_block(Arc::clone(&store), key_b).await.is_ok());

    let reloaded = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    check!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![key_a.to_string()]
    );
    check!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[2; 16], 0, 1_000)
            == vec![key_b.to_string()]
    );
    // One of the two lost the race for the generation and wrote again.
    check!(barrier.snapshot_put_count() > 2);
}

/// One WAL partition window flushed through a builder that carries `index`,
/// the way `run_block_builder` does between polls.
async fn flush_one_window(
    store: &Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    index: &mut TraceIndex,
    partition: i32,
    offset: i64,
    trace_id: [u8; 16],
    start_ns: i64,
) {
    let windows = decode_consumer_records(&[consumer_record(
        partition,
        offset,
        &rec("tenant-a", trace_id, 1, None, start_ns),
    )])
    .unwrap();
    let writer = BlockWriter::new(Arc::clone(store));
    flush_partition_windows(&writer, index, Arc::clone(store), config, windows)
        .await
        .unwrap();
}

fn block_key(partition: i32, offset: i64, start_ns: i64) -> String {
    format!("traces/tenant-a/{partition:05}/{offset:020}-{offset:020}-{start_ns}.parquet")
}

/// Every block the index names, sorted, which is exact where a trace-id lookup
/// would also admit a bloom false positive.
fn indexed_block_keys(index: &TraceIndex) -> Vec<String> {
    let mut keys: Vec<String> = index
        .trace_blocks("tenant-a")
        .iter()
        .map(|block| block.object_key.clone())
        .collect();
    keys.sort();
    keys
}

async fn snapshot_object_count(store: &Arc<dyn ObjectStore>, index_key: &str) -> usize {
    use futures::StreamExt as _;

    let prefix = Path::from(krabka_blockstore::index_snapshot_prefix_for_key(index_key));
    let mut stream = store.list(Some(&prefix));
    let mut count = 0;
    while let Some(meta) = stream.next().await {
        meta.unwrap();
        count += 1;
    }
    count
}

/// The two-writer race is the smallest one. Three replicas, each owning its own
/// WAL partition and its own in-memory index, all reach the gate before any of
/// them writes, so two of the three must lose the conditional create and fold
/// into the winner's snapshot rather than over it.
#[tokio::test]
async fn three_concurrent_block_builders_sharing_one_index_key_keep_every_block_queryable() {
    let gate = Arc::new(tokio::sync::Barrier::new(3));
    let barrier = Arc::new(IndexSnapshotBarrierStore::new(
        "index/traces/snapshots",
        gate,
        3,
    ));
    let store: Arc<dyn ObjectStore> = Arc::clone(&barrier) as Arc<dyn ObjectStore>;
    let config = block_builder_config();

    let builders = [
        (3, 10, [1; 16], 100),
        (4, 20, [2; 16], 200),
        (5, 30, [3; 16], 300),
    ]
    .map(|(partition, offset, trace_id, start_ns)| {
        let store = Arc::clone(&store);
        let config = &config;
        async move {
            let mut index = TraceIndex::new();
            flush_one_window(
                &store, config, &mut index, partition, offset, trace_id, start_ns,
            )
            .await;
        }
    });
    let [first, second, third] = builders;
    tokio::join!(first, second, third);

    let expected = vec![
        block_key(3, 10, 100),
        block_key(4, 20, 200),
        block_key(5, 30, 300),
    ];
    for key in &expected {
        check!(read_block(Arc::clone(&store), key).await.is_ok());
    }

    let reloaded = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    check!(indexed_block_keys(&reloaded) == expected);
    // Two of the three lost the race for a generation and wrote again.
    check!(barrier.snapshot_put_count() > 3);
}

/// A builder that restarts reloads the snapshot and keeps flushing. What it
/// publishes next must still name the blocks its peer wrote while it was down,
/// so the loss cannot creep back in one restart at a time.
#[tokio::test]
async fn a_restarted_block_builder_keeps_a_concurrent_writers_blocks() {
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let store: Arc<dyn ObjectStore> = Arc::new(IndexSnapshotBarrierStore::new(
        "index/traces/snapshots",
        gate,
        2,
    ));
    let config = block_builder_config();

    let first = {
        let store = Arc::clone(&store);
        let config = &config;
        async move {
            let mut index = TraceIndex::new();
            flush_one_window(&store, config, &mut index, 3, 10, [1; 16], 100).await;
        }
    };
    let second = {
        let store = Arc::clone(&store);
        let config = &config;
        async move {
            let mut index = TraceIndex::new();
            flush_one_window(&store, config, &mut index, 4, 20, [2; 16], 200).await;
        }
    };
    tokio::join!(first, second);

    // Partition 3's builder comes back up and flushes its next window.
    let mut restarted = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    flush_one_window(&store, &config, &mut restarted, 3, 11, [4; 16], 400).await;

    let reloaded = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    check!(
        indexed_block_keys(&reloaded)
            == vec![
                block_key(3, 10, 100),
                block_key(3, 11, 400),
                block_key(4, 20, 200),
            ]
    );
}

/// Retention still bounds the snapshot set under contention, and pruning down
/// to two generations never strands a writer: one whose merge base is deleted
/// before it can read it starts over from whatever is newest.
#[tokio::test]
async fn retention_still_prunes_when_four_builders_write_at_once() {
    let gate = Arc::new(tokio::sync::Barrier::new(4));
    let barrier = Arc::new(IndexSnapshotBarrierStore::new(
        "index/traces/snapshots",
        gate,
        4,
    ));
    let store: Arc<dyn ObjectStore> = Arc::clone(&barrier) as Arc<dyn ObjectStore>;
    let retain = krabka_blockstore::IndexSnapshotRetain::new(2).unwrap();
    let config = BlockBuilderConfig {
        index_snapshot_retain: retain,
        ..block_builder_config()
    };

    let builders = [
        (3, 10, [1; 16], 100),
        (4, 20, [2; 16], 200),
        (5, 30, [3; 16], 300),
        (6, 40, [4; 16], 400),
    ]
    .map(|(partition, offset, trace_id, start_ns)| {
        let store = Arc::clone(&store);
        let config = &config;
        async move {
            let mut index = TraceIndex::new();
            flush_one_window(
                &store, config, &mut index, partition, offset, trace_id, start_ns,
            )
            .await;
        }
    });
    let [first, second, third, fourth] = builders;
    tokio::join!(first, second, third, fourth);

    check!(snapshot_object_count(&store, &config.index_key).await == retain.into_value());

    let reloaded = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    check!(
        indexed_block_keys(&reloaded)
            == vec![
                block_key(3, 10, 100),
                block_key(4, 20, 200),
                block_key(5, 30, 300),
                block_key(6, 40, 400),
            ]
    );
    // Three of the four lost the race for a generation and wrote again.
    check!(barrier.snapshot_put_count() > 4);
}

/// Object store that holds one trace-index manifest `put` until the test lets
/// it go.
///
/// [`IndexSnapshotBarrierStore`] makes two writers overlap but leaves the order
/// they land in to the scheduler. A compaction race needs more than overlap: it
/// needs the *other* writer's snapshot to land while this one is already inside
/// its write, holding bytes it merged from a base that has since gone stale.
/// This store hands the test a signal when it is holding a put and waits for
/// one back, so that interleaving is a fact of the test rather than a hope
/// about the scheduler, and no sleep is involved.
struct SnapshotHandoffStore {
    inner: Arc<InMemory>,
    snapshot_prefix: String,
    held: StdMutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    snapshot_puts: AtomicUsize,
}

/// The two ends of one held snapshot write.
struct SnapshotHandoff {
    /// Resolves once a snapshot write is waiting at the door.
    reached: oneshot::Receiver<()>,
    /// Lets that write through.
    release: oneshot::Sender<()>,
}

impl SnapshotHandoffStore {
    fn new(snapshot_prefix: &str) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            snapshot_prefix: snapshot_prefix.to_string(),
            held: StdMutex::new(None),
            snapshot_puts: AtomicUsize::new(0),
        }
    }

    /// Arms the store to hold the next snapshot write.
    ///
    /// Seeding a starting generation is a snapshot write like any other, so
    /// arming is separate from construction: a test seeds first and arms once
    /// the writer it wants to catch is the next one through.
    fn arm(&self) -> SnapshotHandoff {
        let (reached_tx, reached) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *self.held.lock().expect("handoff lock") = Some((reached_tx, release_rx));
        SnapshotHandoff { reached, release }
    }

    /// Snapshot writes attempted, retries included.
    fn snapshot_put_count(&self) -> usize {
        self.snapshot_puts.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for SnapshotHandoffStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SnapshotHandoffStore")
    }
}

impl std::fmt::Display for SnapshotHandoffStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SnapshotHandoffStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for SnapshotHandoffStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with(&self.snapshot_prefix) {
            self.snapshot_puts.fetch_add(1, Ordering::SeqCst);
            // Taken, not held: the lock must not span the wait, or the writer
            // that is meant to overtake this one could not reach the store.
            let held = self.held.lock().expect("handoff lock").take();
            if let Some((reached, release)) = held {
                let _ = reached.send(());
                let _ = release.await;
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A trace-block record with a bloom that holds exactly `trace_ids`.
fn trace_block_stats(
    object_key: &str,
    min_ts: i64,
    max_ts: i64,
    trace_ids: &[[u8; 16]],
) -> TraceBlockStats {
    let mut bloom = ShardedTraceBloom::new(1, 64, 0.01);
    for trace_id in trace_ids {
        bloom.insert(trace_id);
    }
    TraceBlockStats {
        object_key: object_key.to_string(),
        min_ts,
        max_ts,
        bloom,
        tag_names: BTreeSet::new(),
        tag_values: BTreeMap::new(),
        row_count: 0,
        level: BlockLevel::INGESTED,
    }
}

/// The M3 race, in the order that makes it bite.
///
/// A block builder and a compactor each hold their own in-memory `TraceIndex`,
/// both naming the same ingested block. The compactor replaces that block, and
/// its snapshot lands while the builder is already inside a write it merged
/// from the older base. The builder has no removal to replay, because it never
/// made one, so a merge that contributed every block the builder names would
/// union the compaction's input back in beside its output, and every trace in
/// it would be read twice until the builder restarted.
///
/// The builder's write loses the conditional create and merges again, this time
/// against the compactor's snapshot, and contributes only the block it has
/// written since its last successful write.
#[tokio::test]
async fn a_builder_merge_does_not_resurrect_the_block_a_compactor_replaced() {
    let handoff_store = Arc::new(SnapshotHandoffStore::new("index/traces/snapshots"));
    let store: Arc<dyn ObjectStore> = Arc::clone(&handoff_store) as Arc<dyn ObjectStore>;
    let config = block_builder_config();

    // The builder publishes one block, so builder and compactor both name it
    // from the same durable snapshot.
    let mut builder_index = TraceIndex::new();
    flush_one_window(&store, &config, &mut builder_index, 3, 10, [1; 16], 100).await;
    let input_key = block_key(3, 10, 100);
    check!(indexed_block_keys(&builder_index) == vec![input_key.clone()]);

    // The compactor is another process: it loads that snapshot and swaps the
    // ingested block for a compacted one.
    let compacted_key = "traces/tenant-a/compacted/l1-100-100-000000000000002a.parquet";
    let mut compactor_index = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    compactor_index.replace_trace_blocks(
        "tenant-a",
        std::slice::from_ref(&input_key),
        trace_block_stats(compacted_key, 100, 100, &[[1; 16]]),
    );

    // The builder starts its next save and is held inside the put, having
    // already merged a base that still names the input block.
    let handoff = handoff_store.arm();
    let builder = async {
        flush_one_window(&store, &config, &mut builder_index, 3, 11, [2; 16], 200).await;
    };
    let compactor = async {
        handoff.reached.await.unwrap();
        compactor_index
            .save_latest_snapshot(&store, &config.index_key)
            .await
            .unwrap();
        handoff.release.send(()).unwrap();
    };
    tokio::join!(builder, compactor);

    let next_key = block_key(3, 11, 200);
    let reloaded = TraceIndex::load_latest_snapshot(&store, &config.index_key)
        .await
        .unwrap();
    check!(indexed_block_keys(&reloaded) == vec![next_key, compacted_key.to_string()]);
    // The compacted-away input is gone, so its trace is found once.
    check!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![compacted_key.to_string()]
    );
    // Seed, the builder's rejected write, the compactor's, the builder's
    // retry: the interleaving happened rather than the two writes lining up.
    check!(handoff_store.snapshot_put_count() == 4);
}

/// The other direction, and the one that rules out publishing a stale
/// compaction output.
///
/// Object keys are derived from what a block holds, not minted, so the same key
/// can be handed out again for a different input set, which is exactly what
/// `planned_compacted_object_key` allows. Here a writer publishes a new block under the
/// key a compactor has just retired, and the compactor's merge lands after it.
///
/// A removal recorded by name alone would drop the reused block. Keeping both
/// records would double-count the retired contents through the compaction
/// output. The conditional snapshot retry must instead detect that its pinned
/// input record changed and reject the whole replacement.
#[tokio::test]
async fn a_reused_input_key_rejects_the_stale_compactor_output() {
    let handoff_store = Arc::new(SnapshotHandoffStore::new("index/traces/snapshots"));
    let store: Arc<dyn ObjectStore> = Arc::clone(&handoff_store) as Arc<dyn ObjectStore>;
    let index_key = "index/traces.json";
    let reused_key = "traces/tenant-a/compacted/l1-100-200-0000000000000001.parquet";
    let output_key = "traces/tenant-a/compacted/l2-100-200-0000000000000002.parquet";

    // Generation zero names the block the compactor is about to retire.
    let mut seed = TraceIndex::new();
    seed.add_trace_block(
        "tenant-a",
        trace_block_stats(reused_key, 100, 200, &[[1; 16]]),
    );
    seed.save_latest_snapshot(&store, index_key).await.unwrap();

    let mut compactor_index = TraceIndex::load_latest_snapshot(&store, index_key)
        .await
        .unwrap();
    compactor_index.replace_trace_blocks(
        "tenant-a",
        &[reused_key.to_string()],
        trace_block_stats(output_key, 100, 200, &[[1; 16]]),
    );

    // A second writer mints the same key for a different set of traces and a
    // later time range, and publishes it while the compactor is held.
    let mut reuser = TraceIndex::new();
    reuser.add_trace_block(
        "tenant-a",
        trace_block_stats(reused_key, 300, 400, &[[9; 16]]),
    );

    let handoff = handoff_store.arm();
    let compactor = async {
        compactor_index
            .save_latest_snapshot(&store, index_key)
            .await
    };
    let reuser_write = async {
        handoff.reached.await.unwrap();
        reuser
            .save_latest_snapshot(&store, index_key)
            .await
            .unwrap();
        handoff.release.send(()).unwrap();
    };
    let (compactor_result, ()) = tokio::join!(compactor, reuser_write);

    assert2::assert!(matches!(
        compactor_result,
        Err(krabka_blockstore::BlockStoreError::InvalidBlock(message))
            if message.contains(reused_key)
    ));

    let reloaded = TraceIndex::load_latest_snapshot(&store, index_key)
        .await
        .unwrap();
    check!(indexed_block_keys(&reloaded) == vec![reused_key.to_string()]);
    // The live block under the reused key is the new one: its traces and its
    // time range, not the retired record's.
    check!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[9; 16], 300, 400)
            == vec![reused_key.to_string()]
    );
    // The stale output was never published, so the retired record's trace is
    // absent rather than duplicated beside the reused input.
    check!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[1; 16], 100, 200) == Vec::<String>::new()
    );
    // Seed, the compactor's rejected conditional write, and the reuser's
    // winning write. Revalidation rejects the retry before another put.
    check!(handoff_store.snapshot_put_count() == 3);
}
