mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use assert2::assert;
use async_trait::async_trait;
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogLabels, LogRow,
    TimeRange, labels, write_log_block, write_log_block_to_object_store,
};
use krabka_logql::{
    MetricQuery, StreamPlan, StreamQuery, parse_metric_query, parse_query, plan_stream_query,
};
use krabka_observability::{
    BufferedLogHotTail, CompactionFrontier, KafkaWalHeader, KafkaWalRecord, LogWalConsumer, Offset,
    PartitionIndex, QueryError, WalConsumerError, WalLogRecord, WalPosition,
    build_kafka_wal_record, execute_metric_query, execute_metric_query_from_object_store,
    execute_metric_query_range, execute_metric_query_range_with_hot_tail, execute_stream_query,
    execute_stream_query_from_object_store, execute_stream_query_with_hot_tail,
    execute_stream_query_with_hot_tail_frontier, execute_tail_query,
    execute_tail_query_with_frontier, metric_plan_scan_sql, poll_log_hot_tail_once,
    stream_plan_scan_sql,
};
use krabka_units::{Time, millis};
use object_store::{ObjectStore, local::LocalFileSystem, path::Path as ObjectPath};
use serde_json::{Value, json};
use support::{BlockSpan, LogEntry, log_entry};

const TENANT: &str = "tenant-a";
const API_PROD: [(&str, &str); 2] = [("app", "api"), ("env", "prod")];
const API_JSON: [(&str, &str); 2] = [("app", "api"), ("format", "json")];
const COST_ROWS: [LogEntry; 3] = [
    log_entry(10, "cost=7"),
    log_entry(20, "cost=5"),
    log_entry(25, "cost=bad"),
];
const SPREAD_COST_ROWS: [LogEntry; 4] = [
    log_entry(10, "cost=2"),
    log_entry(20, "cost=4"),
    log_entry(25, "cost=4"),
    log_entry(29, "cost=bad"),
];
const ERROR_ROWS: [LogEntry; 3] = [
    log_entry(10, "api error one"),
    log_entry(19, "api error two"),
    log_entry(29, "api error three"),
];
const GROWING_ROWS: [LogEntry; 3] = [
    log_entry(10, "aa"),
    log_entry(19, "bbb"),
    log_entry(29, "cccc"),
];
const NESTED_JSON_LINE: &str = r#"{"request":{"method":"GET"},"response":{"status":500}}"#;
const RATE_PLAN: TimeRange = TimeRange {
    start_ns: -20_000_000_000,
    end_ns: 30_000_000_000,
};
const STEPPED_PLAN: TimeRange = TimeRange {
    start_ns: -10,
    end_ns: 30,
};
const MISSING_BLOCK_WARNING: &str =
    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet";

/// Cold blocks for one tenant, on local disk and in an object store rooted at
/// the same directory.
struct Fixture {
    dir: tempfile::TempDir,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    label_index: LabelIndex,
    block_index: BlockIndex,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        Self {
            dir,
            store,
            prefix: ObjectPath::from("observability/logs"),
            label_index: LabelIndex::default(),
            block_index: BlockIndex::default(),
        }
    }

    fn series(&mut self, series_labels: LogLabels) -> u64 {
        self.label_index.insert_series(TENANT, series_labels)
    }

    fn block(&mut self, partition: PartitionIndex, span: BlockSpan, rows: Vec<LogRow>) {
        let block = write_log_block(self.dir.path(), &block_key(partition, span), rows).unwrap();
        self.block_index.insert(block);
    }

    async fn object_block(
        &mut self,
        partition: PartitionIndex,
        span: BlockSpan,
        rows: Vec<LogRow>,
    ) {
        let block = write_log_block_to_object_store(
            self.store.as_ref(),
            &self.prefix,
            &block_key(partition, span),
            rows,
        )
        .await
        .unwrap();
        self.block_index.insert(block);
    }

    fn missing_block(&mut self, span: BlockSpan, series: u64) {
        self.block_index.insert(BlockDescriptor::new(
            block_key(PartitionIndex(0), span),
            BTreeSet::from([series]),
        ));
    }

    fn plan(&self, range: TimeRange, query: StreamQuery) -> StreamPlan {
        plan_stream_query(TENANT, range, query, &self.label_index, &self.block_index).unwrap()
    }

    fn stream_plan(&self, query: &str) -> StreamPlan {
        self.plan(WHOLE_FIXTURE, parse_query(query).unwrap())
    }

    fn metric_plan(&self, range: TimeRange, query: &str) -> (StreamPlan, MetricQuery) {
        let query = parse_metric_query(query).unwrap();
        (self.plan(range, query.stream.clone()), query)
    }

    async fn stream(&self, query: &str) -> Value {
        execute_stream_query(self.dir.path(), &self.stream_plan(query), &self.label_index)
            .await
            .unwrap()
    }

    async fn object_store_stream(&self, query: &str) -> Value {
        execute_stream_query_from_object_store(
            Arc::clone(&self.store),
            &self.prefix,
            &self.stream_plan(query),
            &self.label_index,
        )
        .await
        .unwrap()
    }

    async fn metric(&self, query: &str) -> Result<Value, QueryError> {
        let (plan, query) = self.metric_plan(WHOLE_FIXTURE, query);
        execute_metric_query(self.dir.path(), &plan, &query, &self.label_index).await
    }

    async fn metric_range(&self, range_query: &RangeQuery<'_>) -> Value {
        let (plan, query) = self.metric_plan(range_query.plan, range_query.query);
        execute_metric_query_range(
            self.dir.path(),
            &plan,
            &query,
            &self.label_index,
            range_query.eval,
            range_query.step_ns,
        )
        .await
        .unwrap()
    }

    async fn metric_with_hot_tail(
        &self,
        hot_query: &HotTailQuery<'_>,
    ) -> Result<Value, QueryError> {
        let (plan, query) = self.metric_plan(hot_query.plan, hot_query.query);
        execute_metric_query_range_with_hot_tail(
            self.dir.path(),
            &plan,
            &query,
            &self.label_index,
            (TimeRange::new(30, 30).unwrap(), 1),
            hot_query.hot_tail,
            hot_query.compacted_through_ns,
        )
        .await
    }
}

/// The whole time range the fixtures cover, in nanoseconds.
const WHOLE_FIXTURE: TimeRange = TimeRange {
    start_ns: 0,
    end_ns: 30,
};

/// A metric range query over the fixture's cold blocks.
struct RangeQuery<'a> {
    query: &'a str,
    /// The range the stream plan selects blocks for.
    plan: TimeRange,
    /// The range the query is evaluated over.
    eval: TimeRange,
    step_ns: i64,
}

/// A metric query at 30 ns over the fixture's cold blocks and a hot tail.
struct HotTailQuery<'a> {
    query: &'a str,
    /// The range the stream plan selects blocks for.
    plan: TimeRange,
    hot_tail: &'a [WalLogRecord],
    /// The hot tail holds only records after this timestamp.
    compacted_through_ns: i64,
}

/// One pod's sample at 30 ns in a `pod_matrix`.
struct PodSample<'a> {
    env: &'a str,
    pod: &'a str,
    sample_value: &'a str,
}

/// Sets one field of a JSON object.
trait WithField {
    fn with_field(self, key: &str, field_value: Value) -> Self;
}

impl WithField for Value {
    fn with_field(mut self, key: &str, field_value: Value) -> Self {
        self[key] = field_value;
        self
    }
}

fn block_key(partition: PartitionIndex, span: BlockSpan) -> BlockKey {
    BlockKey::new(
        TENANT,
        partition.0,
        span.first,
        span.last,
        TimeRange::new(span.first, span.last).unwrap(),
    )
}

fn row(series: u64, entry: LogEntry<'_>) -> LogRow {
    LogRow::new(series, entry.timestamp_ns, entry.line, BTreeMap::new())
}

fn rows(series: u64, entries: &[LogEntry<'_>]) -> Vec<LogRow> {
    entries.iter().map(|&entry| row(series, entry)).collect()
}

fn single_series(series_labels: LogLabels, span: BlockSpan, entries: &[LogEntry<'_>]) -> Fixture {
    let mut fixture = Fixture::new();
    let series = fixture.series(series_labels);
    fixture.block(PartitionIndex(0), span, rows(series, entries));
    fixture
}

fn api(span: BlockSpan, entries: &[LogEntry<'_>]) -> Fixture {
    single_series(labels(API_PROD), span, entries)
}

fn api_and_worker(api_span: BlockSpan, api_entries: &[LogEntry<'_>]) -> Fixture {
    let mut fixture = Fixture::new();
    let api = fixture.series(labels(API_PROD));
    let worker = fixture.series(labels([("app", "worker"), ("env", "prod")]));
    fixture.block(PartitionIndex(0), api_span, rows(api, api_entries));
    fixture.block(
        PartitionIndex(1),
        BlockSpan {
            first: 20,
            last: 29,
        },
        rows(worker, &[log_entry(25, "worker error")]),
    );
    fixture
}

fn prod_pods<const N: usize>(fixture: &mut Fixture, pods: [&str; N]) -> [u64; N] {
    pods.map(|pod| fixture.series(labels([("app", "api"), ("env", "prod"), ("pod", pod)])))
}

fn hot(tenant: &str, stream_labels: LogLabels, entry: LogEntry<'_>) -> WalLogRecord {
    WalLogRecord {
        tenant: tenant.to_string(),
        labels: stream_labels,
        timestamp_ns: entry.timestamp_ns,
        line: entry.line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

fn api_hot(timestamp_ns: i64, line: &str) -> WalLogRecord {
    hot(TENANT, labels(API_PROD), log_entry(timestamp_ns, line))
}

fn positioned(record: WalLogRecord, offset: i64) -> WalLogRecord {
    WalLogRecord {
        position: Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(offset),
        }),
        ..record
    }
}

fn frontier_hot_tail() -> (Vec<WalLogRecord>, CompactionFrontier) {
    (
        vec![
            positioned(api_hot(20, "already compacted error"), 42),
            positioned(api_hot(21, "new hot error"), 43),
        ],
        CompactionFrontier::new(19).with_partition_offset(PartitionIndex(0), Offset(42)),
    )
}

fn fingerprints(plan: &StreamPlan) -> String {
    plan.fingerprints
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn success(result_type: &str, data_result: Value) -> Value {
    let mut body = json!({
        "status": "success",
        "data": {
            "resultType": result_type
        }
    });
    body["data"]["result"] = data_result;
    body
}

fn streams(streams_json: Value) -> Value {
    success("streams", streams_json)
}

fn matrix(series_json: Value) -> Value {
    success("matrix", series_json)
}

fn api_prod_stream(values: Value) -> Value {
    json!({ "stream": { "app": "api", "env": "prod" } }).with_field("values", values)
}

fn api_stream(values: Value) -> Value {
    streams(Value::Array(vec![api_prod_stream(values)]))
}

fn api_tail_frame(values: Value) -> Value {
    json!({ "streams": [api_prod_stream(values)] })
}

fn api_matrix(values: Value) -> Value {
    matrix(Value::Array(vec![
        json!({ "metric": { "app": "api", "env": "prod" } }).with_field("values", values),
    ]))
}

fn api_matrix_at_30(sample_value: &str) -> Value {
    api_matrix(json!([[0.000_000_03, sample_value]]))
}

fn env_matrix(values: Value) -> Value {
    matrix(Value::Array(vec![
        json!({ "metric": { "env": "prod" } }).with_field("values", values),
    ]))
}

fn with_warning(mut response: Value) -> Value {
    response["warnings"] = json!([MISSING_BLOCK_WARNING]);
    response
}

#[tokio::test]
async fn executes_stream_query_over_planned_cold_blocks_as_loki_json() {
    let fixture = api_and_worker(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(10, "api ok"), log_entry(19, "api error")],
    );

    let response = fixture.stream(r#"{app="api"} |= "error""#).await;

    assert!(response == api_stream(json!([["19", "api error"]])));
}

#[tokio::test]
async fn literal_line_filter_pushdown_treats_like_wildcards_as_plain_text() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, "api cpu 100% ok"),
            log_entry(11, "api cpu 1000 ok"),
            log_entry(12, "api shard a_b ok"),
            log_entry(13, "api shard acb ok"),
        ],
    );

    let percent_response = fixture.stream(r#"{app="api"} |= "100%""#).await;
    assert!(percent_response["data"]["result"][0]["values"] == json!([["10", "api cpu 100% ok"]]));

    let underscore_response = fixture.stream(r#"{app="api"} |= "a_b""#).await;
    assert!(
        underscore_response["data"]["result"][0]["values"] == json!([["12", "api shard a_b ok"]])
    );
}

#[tokio::test]
async fn executes_stream_query_merging_cold_blocks_with_hot_wal_tail() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(10, "api ok"), log_entry(19, "api error")],
    );
    let plan = fixture.stream_plan(r#"{app="api"} |= "error""#);
    let hot_tail = vec![
        api_hot(19, "api error"),
        api_hot(20, "api hot error"),
        hot(
            "tenant-b",
            labels(API_PROD),
            log_entry(21, "other tenant error"),
        ),
    ];

    let response = execute_stream_query_with_hot_tail(
        fixture.dir.path(),
        &plan,
        &fixture.label_index,
        &hot_tail,
        19,
    )
    .await
    .unwrap();

    assert!(response == api_stream(json!([["19", "api error"], ["20", "api hot error"]])));
}

#[test]
fn stream_plan_scan_sql_pushes_down_time_fingerprints_and_literal_line_filters() {
    let fixture = api_and_worker(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(19, "api error")],
    );
    let plan = fixture.stream_plan(r#"{app=~".+"} |= "error" != "debug""#);

    let fingerprints = fingerprints(&plan);
    assert!(
        stream_plan_scan_sql(&plan)
            == format!(
                "select series_fingerprint, timestamp_ns, line, structured_metadata \
                 from logs \
                 where timestamp_ns >= 0 and timestamp_ns <= 30 \
                 and series_fingerprint in ({fingerprints}) \
                 and line like '%error%' \
                 and line not like '%debug%' \
                 order by series_fingerprint, timestamp_ns"
            )
    );
}

#[test]
fn metric_plan_scan_sql_uses_eval_range_selector_and_stream_pushdowns() {
    let fixture = api_and_worker(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(19, "api error")],
    );
    let (plan, query) = fixture.metric_plan(
        TimeRange {
            start_ns: 0,
            end_ns: 40,
        },
        r#"count_over_time({app=~".+"} |= "error" [20ns])"#,
    );
    let fingerprints = fingerprints(&plan);

    assert!(
        metric_plan_scan_sql(&plan, &query, TimeRange::new(30, 40).unwrap()).unwrap()
            == format!(
                "select series_fingerprint, timestamp_ns, line, structured_metadata \
                 from logs \
                 where timestamp_ns >= 10 and timestamp_ns <= 40 \
                 and series_fingerprint in ({fingerprints}) \
                 and line like '%error%' \
                 order by series_fingerprint, timestamp_ns"
            )
    );
}

#[tokio::test]
async fn executes_stream_query_filters_hot_tail_by_partition_offset_frontier() {
    let fixture = Fixture::new();
    let plan = fixture.stream_plan(r#"{app="api"} |= "error""#);
    let (hot_tail, frontier) = frontier_hot_tail();

    let response = execute_stream_query_with_hot_tail_frontier(
        fixture.dir.path(),
        &plan,
        &fixture.label_index,
        &hot_tail,
        &frontier,
    )
    .await
    .unwrap();

    assert!(response == api_stream(json!([["21", "new hot error"]])));
}

#[tokio::test]
async fn hot_tail_buffer_polls_and_decodes_kafka_wal_records() {
    let record = WalLogRecord {
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        ..api_hot(1_900_000, "api error")
    };
    let produced = build_kafka_wal_record("__krabka_observability_logs_wal", &record).unwrap();
    let mut consumer = RecordingWalConsumer::new(vec![vec![
        KafkaWalRecord {
            value: produced.value.unwrap().to_vec(),
            partition: PartitionIndex(2),
            offset: Offset(42),
            timestamp_ms: produced.timestamp_ms,
            headers: produced
                .headers
                .into_iter()
                .map(|header| KafkaWalHeader {
                    key: header.key,
                    value: header.value.map(|value| value.to_vec()),
                })
                .collect(),
        },
        KafkaWalRecord {
            value: b"worker error".to_vec(),
            partition: PartitionIndex(3),
            offset: Offset(7),
            timestamp_ms: Some(2),
            headers: vec![
                kafka_header("krabka-wal-record-type", "log-line"),
                kafka_header("krabka-tenant", "tenant-a"),
                kafka_header("krabka-log-label-app", "worker"),
            ],
        },
    ]]);
    let hot_tail = BufferedLogHotTail::default();

    let decoded = poll_log_hot_tail_once(&mut consumer, &hot_tail, millis(1))
        .await
        .unwrap();

    assert!(decoded == 2);
    assert!(
        hot_tail.records()
            == vec![
                WalLogRecord {
                    position: Some(WalPosition {
                        partition: PartitionIndex(2),
                        offset: Offset(42),
                    }),
                    ..record
                },
                WalLogRecord {
                    position: Some(WalPosition {
                        partition: PartitionIndex(3),
                        offset: Offset(7),
                    }),
                    ..hot(
                        TENANT,
                        labels([("app", "worker")]),
                        log_entry(2_000_000, "worker error")
                    )
                },
            ]
    );
}

#[test]
fn executes_tail_query_over_hot_wal_tail_as_loki_streams_json_frame() {
    let plan = Fixture::new().stream_plan(r#"{app="api"} |= "error""#);
    let hot_tail = vec![
        api_hot(19, "api cold error"),
        api_hot(20, "api hot error"),
        hot(
            "tenant-b",
            labels(API_PROD),
            log_entry(21, "other tenant error"),
        ),
        hot(
            TENANT,
            labels([("app", "worker"), ("env", "prod")]),
            log_entry(22, "worker error"),
        ),
    ];

    let response = execute_tail_query(&plan, &hot_tail, 19);

    assert!(response == api_tail_frame(json!([["20", "api hot error"]])));
}

#[test]
fn executes_tail_query_filters_hot_tail_by_partition_offset_frontier() {
    let plan = Fixture::new().stream_plan(r#"{app="api"} |= "error""#);
    let (hot_tail, frontier) = frontier_hot_tail();

    let response = execute_tail_query_with_frontier(&plan, &hot_tail, &frontier);

    assert!(response == api_tail_frame(json!([["21", "new hot error"]])));
}

#[tokio::test]
async fn executes_stream_query_with_json_field_filter_over_structured_metadata() {
    let mut fixture = Fixture::new();
    let api = fixture.series(labels(API_PROD));
    let status = |code: &str| BTreeMap::from([("status".to_string(), code.to_string())]);
    fixture.block(
        PartitionIndex(0),
        BlockSpan {
            first: 10,
            last: 19,
        },
        vec![
            LogRow::new(api, 10, "api ok", status("200")),
            LogRow::new(api, 19, "api error", status("500")),
        ],
    );

    let response = fixture.stream(r#"{app="api"} | status >= 500"#).await;

    assert!(
        response
            == streams(json!([
                {
                    // The metadata the filter matched on is folded
                    // into the stream's labels, which is where Loki's
                    // default encoding leaves it.
                    "stream": {
                        "app": "api",
                        "env": "prod",
                        "status": "500"
                    },
                    "values": [
                        ["19", "api error"]
                    ]
                }
            ]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_field_filter_over_original_labels() {
    let mut fixture = Fixture::new();
    let api = fixture.series(labels(API_PROD));
    let api_dev = fixture.series(labels([("app", "api"), ("env", "dev")]));
    fixture.block(
        PartitionIndex(0),
        BlockSpan {
            first: 10,
            last: 19,
        },
        vec![
            LogRow::new(api, 10, "api prod error", BTreeMap::new()),
            LogRow::new(api_dev, 19, "api dev error", BTreeMap::new()),
        ],
    );

    let response = fixture.stream(r#"{app="api"} | env = "prod""#).await;

    assert!(response == api_stream(json!([["10", "api prod error"]])));
}

#[tokio::test]
async fn executes_stream_query_with_extracted_label_collision_suffix() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, r#"{"env":"stage","status":200}"#),
            log_entry(19, r#"{"env":"dev","status":500}"#),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | json | env = "prod" | env_extracted = "dev""#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "env_extracted": "dev",
                    "status": "500"
                },
                "values": [
                    ["19", r#"{"env":"dev","status":500}"#]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_nested_json_field_filter_over_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(
                10,
                r#"{"request":{"method":"GET"},"response":{"status":200}}"#,
            ),
            log_entry(19, NESTED_JSON_LINE),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | json | request_method = "GET" | response_status >= 500"#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "request_method": "GET",
                    "response_status": "500"
                },
                "values": [
                    ["19", NESTED_JSON_LINE]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_selected_json_field_filter_over_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(
                10,
                r#"{"servers":["10.0.0.1"],"request":{"headers":{"User-Agent":"Agent/1"},"method":"GET"},"status":200}"#,
            ),
            log_entry(
                19,
                r#"{"servers":["10.0.0.2"],"request":{"headers":{"User-Agent":"Agent/2"},"method":"POST"},"status":500}"#,
            ),
        ],
    );

    let response = fixture
        .stream(
            r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua = "Agent/2""#,
        )
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "first_server": "10.0.0.2",
                    "ua": "Agent/2"
                },
                "values": [
                    ["19", r#"{"servers":["10.0.0.2"],"request":{"headers":{"User-Agent":"Agent/2"},"method":"POST"},"status":500}"#]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_logfmt_field_filter_over_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, r#"status=200 msg="api ok""#),
            log_entry(19, r#"status=500 msg="api error""#),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | logfmt | status >= 500"#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "msg": "api error",
                    "status": "500"
                },
                "values": [
                    ["19", r#"status=500 msg="api error""#]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_parameterized_logfmt_field_filter_over_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, r#"status=200 msg="api ok" path=/ready"#),
            log_entry(19, r#"status=500 msg="api error" path=/api"#),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | logfmt status, message="msg" | status >= 500"#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "message": "api error",
                    "status": "500"
                },
                "values": [
                    ["19", r#"status=500 msg="api error" path=/api"#]
                ]
            }]))
    );
}

fn query_range_access_log_stream() -> Value {
    streams(json!([{
        "stream": {
            "app": "api",
            "duration": "1.5s",
            "env": "prod",
            "method": "POST",
            "path": "/api/prom/query_range",
            "status": "500"
        },
        "values": [
            ["19", "POST /api/prom/query_range (500) 1.5s"]
        ]
    }]))
}

fn access_log_fixture() -> Fixture {
    api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, "GET /ready (200) 1ms"),
            log_entry(19, "POST /api/prom/query_range (500) 1.5s"),
        ],
    )
}

#[tokio::test]
async fn executes_stream_query_with_pattern_parser_over_line_body() {
    let response = access_log_fixture()
        .stream(r#"{app="api"} | pattern `<method> <path> (<status>) <duration>` | status >= 500"#)
        .await;

    assert!(response == query_range_access_log_stream());
}

#[tokio::test]
async fn executes_stream_query_with_regexp_parser_over_line_body() {
    let response = access_log_fixture()
        .stream(
            r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)` | status >= 500"#,
        )
        .await;

    assert!(response == query_range_access_log_stream());
}

#[tokio::test]
async fn executes_stream_query_with_unpack_parser_replacing_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(
                10,
                r#"{"container":"myapp","pod":"pod-3223f","_entry":"original log message"}"#,
            ),
            log_entry(
                19,
                r#"{"container":"myapp","pod":"pod-3223f","_entry":"container original log message"}"#,
            ),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | unpack != "container" | pod = "pod-3223f""#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "container": "myapp",
                    "env": "prod",
                    "pod": "pod-3223f"
                },
                "values": [
                    ["10", "original log message"]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_line_format_replacing_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, r#"status=200 msg="api ok""#),
            log_entry(19, r#"status=500 msg="api error""#),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | logfmt | line_format `{{.msg}} {{.status}}` |= "api error 500""#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "msg": "api error",
                    "status": "500"
                },
                "values": [
                    ["19", "api error 500"]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_label_format_rewriting_stream_labels() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, r"method=GET status=200 path=/ready"),
            log_entry(19, r"method=GET status=500 path=/api"),
        ],
    );

    let response = fixture
        .stream(
            r#"{app="api",env="prod"} | logfmt | label_format namespace=env, summary="{{.method}} {{.status}}" | namespace = "prod" | summary = "GET 500""#,
        )
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "method": "GET",
                    "namespace": "prod",
                    "path": "/api",
                    "status": "500",
                    "summary": "GET 500"
                },
                "values": [
                    ["19", "method=GET status=500 path=/api"]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_drop_and_keep_rewriting_stream_labels() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, r"method=GET status=200 level=info path=/ready"),
            log_entry(19, r"method=GET status=500 level=debug path=/api"),
        ],
    );

    let response = fixture
        .stream(
            r#"{app="api",env="prod"} | logfmt | drop env, level="debug" | keep app, method, status="500" | status = "500""#,
        )
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "method": "GET",
                    "status": "500"
                },
                "values": [
                    ["19", "method=GET status=500 level=debug path=/api"]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_decolorize_rewriting_line_body() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, "\u{1b}[32mapi ok\u{1b}[0m"),
            log_entry(19, "\u{1b}[31mapi error\u{1b}[0m"),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | decolorize |= "error" !~ `\x1b\[`"#)
        .await;

    assert!(response == api_stream(json!([["19", "api error"]])));
}

#[tokio::test]
async fn executes_stream_query_with_duration_and_bytes_logfmt_field_filters() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, "duration=10ms bytes_consumed=21MB msg=too-fast"),
            log_entry(15, "duration=25ms bytes_consumed=19MB msg=too-small"),
            log_entry(19, "duration=25ms bytes_consumed=21MB msg=matched"),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | logfmt | duration >= 20ms | bytes_consumed > 20MB"#)
        .await;

    assert!(
        response
            == streams(json!([{
                "stream": {
                    "app": "api",
                    "bytes_consumed": "21MB",
                    "duration": "25ms",
                    "env": "prod",
                    "msg": "matched"
                },
                "values": [
                    ["19", "duration=25ms bytes_consumed=21MB msg=matched"]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stream_query_with_or_logfmt_field_filter_chain() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[
            log_entry(10, "status=200 level=info"),
            log_entry(15, "status=200 level=warn"),
            log_entry(19, "status=500 level=info"),
        ],
    );

    let response = fixture
        .stream(r#"{app="api"} | logfmt | status >= 500 or level = "warn""#)
        .await;

    assert!(
        response
            == streams(json!([
                {
                    "stream": {
                        "app": "api",
                        "env": "prod",
                        "level": "info",
                        "status": "500"
                    },
                    "values": [
                        ["19", "status=500 level=info"]
                    ]
                },
                {
                    "stream": {
                        "app": "api",
                        "env": "prod",
                        "level": "warn",
                        "status": "200"
                    },
                    "values": [
                        ["15", "status=200 level=warn"]
                    ]
                }
            ]))
    );
}

#[tokio::test]
async fn executes_count_over_time_query_as_loki_matrix_json() {
    let fixture = api_and_worker(
        BlockSpan {
            first: 10,
            last: 29,
        },
        &[
            log_entry(10, "api ok"),
            log_entry(19, "api error"),
            log_entry(29, "api error again"),
        ],
    );

    let response = fixture
        .metric(r#"count_over_time({app="api"} |= "error" [30s])"#)
        .await
        .unwrap();

    assert!(response == api_matrix_at_30("2"));
}

#[tokio::test]
async fn executes_absent_over_time_query_for_empty_plan() {
    let response = Fixture::new()
        .metric(r#"absent_over_time({app="api",env="prod"} [30s])"#)
        .await
        .unwrap();

    // Real Loki 3.4.2's `absent_over_time` synthesizes a series from the
    // selector's equality matchers only — no `detected_level` (there is no
    // log line to detect a level from).
    assert!(response == api_matrix_at_30("1"));
}

#[tokio::test]
async fn count_over_time_honors_json_parser_error_filters() {
    let fixture = single_series(
        labels(API_JSON),
        BlockSpan {
            first: 10,
            last: 29,
        },
        &[
            log_entry(10, r#"{"msg":"api ok"}"#),
            log_entry(20, "not json"),
            log_entry(29, r#"{"msg":"api later"}"#),
        ],
    );

    let response = fixture
        .metric(r#"count_over_time({app="api",format="json"} | json | __error__ = "" [30ns])"#)
        .await
        .unwrap();

    assert!(
        response
            == matrix(json!([
                {
                    "metric": {
                        "app": "api",
                        "format": "json",
                        "msg": "api later"
                    },
                    "values": [
                        [0.000_000_03, "1"]
                    ]
                },
                {
                    "metric": {
                        "app": "api",
                        "format": "json",
                        "msg": "api ok"
                    },
                    "values": [
                        [0.000_000_03, "1"]
                    ]
                }
            ]))
    );
}

#[tokio::test]
async fn executes_count_over_time_merging_cold_blocks_with_hot_wal_tail() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(10, "api ok"), log_entry(19, "api error")],
    );
    let hot_tail = [api_hot(19, "api error"), api_hot(20, "api hot error")];

    let response = fixture
        .metric_with_hot_tail(&HotTailQuery {
            query: r#"count_over_time({app="api"} |= "error" [30ns])"#,
            plan: WHOLE_FIXTURE,
            hot_tail: &hot_tail,
            compacted_through_ns: 19,
        })
        .await
        .unwrap();

    assert!(response == api_matrix_at_30("2"));
}

#[tokio::test]
async fn executes_rate_merging_cold_blocks_with_hot_wal_tail() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(10, "api error")],
    );
    let hot_tail = [api_hot(10, "api error"), api_hot(20, "api hot error")];

    let response = fixture
        .metric_with_hot_tail(&HotTailQuery {
            query: r#"rate({app="api"} |= "error" [20s])"#,
            plan: RATE_PLAN,
            hot_tail: &hot_tail,
            compacted_through_ns: 10,
        })
        .await
        .unwrap();

    assert!(response == api_matrix_at_30("0.1"));
}

#[tokio::test]
async fn executes_bytes_rate_merging_cold_blocks_with_hot_wal_tail() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(10, "aa")],
    );
    let hot_tail = [api_hot(10, "aa"), api_hot(20, "bbbb")];

    let response = fixture
        .metric_with_hot_tail(&HotTailQuery {
            query: r#"bytes_rate({app="api"} [20s])"#,
            plan: RATE_PLAN,
            hot_tail: &hot_tail,
            compacted_through_ns: 10,
        })
        .await
        .unwrap();

    assert!(response == api_matrix_at_30("0.3"));
}

#[tokio::test]
async fn executes_bytes_over_time_merging_cold_blocks_with_hot_wal_tail() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 19,
        },
        &[log_entry(10, "aa")],
    );
    let hot_tail = [api_hot(10, "aa"), api_hot(20, "bbbb")];

    let response = fixture
        .metric_with_hot_tail(&HotTailQuery {
            query: r#"bytes_over_time({app="api"} [30ns])"#,
            plan: WHOLE_FIXTURE,
            hot_tail: &hot_tail,
            compacted_through_ns: 10,
        })
        .await
        .unwrap();

    assert!(response == api_matrix_at_30("6"));
}

#[tokio::test]
async fn metric_query_rejects_unfiltered_cold_block_pipeline_errors() {
    let fixture = single_series(
        labels(API_JSON),
        BlockSpan {
            first: 20,
            last: 20,
        },
        &[log_entry(20, "not json")],
    );

    let error = fixture
        .metric(r#"count_over_time({app="api",format="json"} | json [30ns])"#)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("JSONParserErr"));
}

#[tokio::test]
async fn metric_query_rejects_unfiltered_hot_tail_pipeline_errors() {
    let mut fixture = Fixture::new();
    fixture.series(labels(API_JSON));
    let hot_tail = [hot(TENANT, labels(API_JSON), log_entry(20, "not json"))];

    let error = fixture
        .metric_with_hot_tail(&HotTailQuery {
            query: r#"count_over_time({app="api",format="json"} | json [30ns])"#,
            plan: WHOLE_FIXTURE,
            hot_tail: &hot_tail,
            compacted_through_ns: 10,
        })
        .await
        .unwrap_err();

    assert!(error.to_string().contains("JSONParserErr"));
}

#[tokio::test]
async fn executes_parser_metric_query_with_loki_pipeline_labels() {
    let fixture = api(
        BlockSpan {
            first: 10,
            last: 10,
        },
        &[log_entry(10, NESTED_JSON_LINE)],
    );
    let hot_tail = [api_hot(20, NESTED_JSON_LINE)];

    let response = fixture
        .metric_with_hot_tail(&HotTailQuery {
            query: r#"count_over_time({app="api"} | json | response_status >= 500 [30ns])"#,
            plan: WHOLE_FIXTURE,
            hot_tail: &hot_tail,
            compacted_through_ns: 10,
        })
        .await
        .unwrap();

    assert!(
        response
            == matrix(json!([{
                "metric": {
                    "app": "api",
                    "env": "prod",
                    "request_method": "GET",
                    "response_status": "500"
                },
                "values": [
                    [0.000_000_03, "2"]
                ]
            }]))
    );
}

async fn api_unwrap(entries: &[LogEntry<'_>], query: &str) -> Value {
    api(
        BlockSpan {
            first: 10,
            last: 29,
        },
        entries,
    )
    .metric(query)
    .await
    .unwrap()
}

#[tokio::test]
async fn executes_sum_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("12"));
}

#[tokio::test]
async fn executes_sum_over_time_decimal_unwrap_metric_query() {
    let response = api_unwrap(
        &[
            log_entry(10, "cost=1.5"),
            log_entry(20, "cost=2.25"),
            log_entry(25, "cost=bad"),
        ],
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("3.75"));
}

#[tokio::test]
async fn executes_sum_over_time_signed_decimal_unwrap_metric_query() {
    let response = api_unwrap(
        &[
            log_entry(10, "cost=-1.5"),
            log_entry(20, "cost=2.25"),
            log_entry(25, "cost=bad"),
        ],
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("0.75"));
}

#[tokio::test]
async fn executes_sum_over_time_scientific_unwrap_metric_query() {
    let response = api_unwrap(
        &[
            log_entry(10, "cost=1.5e2"),
            log_entry(20, "cost=-2.5e1"),
            log_entry(25, "cost=bad"),
        ],
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("125"));
}

#[tokio::test]
async fn executes_sum_over_time_unwrap_bytes_metric_query() {
    let response = api_unwrap(
        &[
            log_entry(10, "size=1KiB"),
            log_entry(20, "size=2KiB"),
            log_entry(25, "size=wat"),
        ],
        r#"sum_over_time({app="api"} | logfmt | unwrap bytes(size) | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("3072"));
}

#[tokio::test]
async fn executes_sum_over_time_unwrap_duration_metric_query() {
    let response = api_unwrap(
        &[
            log_entry(10, "latency=250ms"),
            log_entry(20, "latency=500ms"),
            log_entry(25, "latency=bad"),
        ],
        r#"sum_over_time({app="api"} | logfmt | unwrap duration(latency) | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("0.75"));
}

#[tokio::test]
async fn executes_rate_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"rate({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("0.4"));
}

#[tokio::test]
async fn executes_rate_counter_unwrap_metric_query_with_reset() {
    let response = api_unwrap(
        &[
            log_entry(10, "requests=3"),
            log_entry(15, "requests=9"),
            log_entry(20, "requests=2"),
            log_entry(25, "requests=5"),
            log_entry(29, "requests=bad"),
        ],
        r#"rate_counter({app="api"} | logfmt | unwrap requests | __error__ = "" [30s])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("0.366666666"));
}

#[tokio::test]
async fn executes_avg_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("6"));
}

#[tokio::test]
async fn executes_metric_query_with_range_offset() {
    let fixture = api(
        BlockSpan { first: 5, last: 30 },
        &[
            log_entry(5, "api error old one"),
            log_entry(10, "api error old two"),
            log_entry(25, "api error current"),
        ],
    );

    let response = fixture
        .metric_range(&RangeQuery {
            query: r#"count_over_time({app="api"} |= "error" [10ns] offset 20ns)"#,
            plan: WHOLE_FIXTURE,
            eval: TimeRange {
                start_ns: 30,
                end_ns: 30,
            },
            step_ns: 1,
        })
        .await;

    assert!(response == api_matrix_at_30("2"));
}

#[tokio::test]
async fn executes_avg_over_time_unwrap_metric_query_with_range_grouping() {
    let mut fixture = Fixture::new();
    let pod_a = fixture.series(labels([("app", "api"), ("pod", "a")]));
    let pod_b = fixture.series(labels([("app", "api"), ("pod", "b")]));
    fixture.block(
        PartitionIndex(0),
        BlockSpan {
            first: 10,
            last: 29,
        },
        vec![
            row(pod_a, log_entry(10, "cost=2")),
            row(pod_a, log_entry(12, "cost=4")),
            row(pod_b, log_entry(20, "cost=100")),
            row(pod_b, log_entry(25, "cost=bad")),
        ],
    );

    let response = fixture
        .metric(
            r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns]) by (app)"#,
        )
        .await
        .unwrap();

    assert!(
        response
            == matrix(json!([{
                "metric": {
                    "app": "api"
                },
                "values": [
                    [0.000_000_03, "35.333333333"]
                ]
            }]))
    );
}

#[tokio::test]
async fn executes_stdvar_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &SPREAD_COST_ROWS,
        r#"stdvar_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("0.888888888"));
}

#[tokio::test]
async fn executes_stddev_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &SPREAD_COST_ROWS,
        r#"stddev_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("0.942809041"));
}

#[tokio::test]
async fn executes_quantile_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &[
            log_entry(10, "cost=1"),
            log_entry(15, "cost=2"),
            log_entry(20, "cost=10"),
            log_entry(25, "cost=100"),
            log_entry(29, "cost=bad"),
        ],
        r#"quantile_over_time(0.75, {app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("32.5"));
}

#[tokio::test]
async fn executes_min_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"min_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("5"));
}

#[tokio::test]
async fn executes_max_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"max_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("7"));
}

#[tokio::test]
async fn executes_first_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"first_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("7"));
}

#[tokio::test]
async fn executes_last_over_time_unwrap_metric_query() {
    let response = api_unwrap(
        &COST_ROWS,
        r#"last_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .await;

    assert!(response == api_matrix_at_30("5"));
}

async fn api_stepped(entries: &[LogEntry<'_>], query: &str, plan_range: TimeRange) -> Value {
    api(
        BlockSpan {
            first: 10,
            last: 29,
        },
        entries,
    )
    .metric_range(&RangeQuery {
        query,
        plan: plan_range,
        eval: TimeRange {
            start_ns: 10,
            end_ns: 30,
        },
        step_ns: 10,
    })
    .await
}

#[tokio::test]
async fn executes_count_over_time_query_with_stepped_matrix_samples() {
    let response = api_stepped(
        &ERROR_ROWS,
        r#"count_over_time({app="api"} |= "error" [20ns])"#,
        STEPPED_PLAN,
    )
    .await;

    assert!(
        response
            == api_matrix(json!([
                [0.000_000_01, "1"],
                [0.000_000_02, "2"],
                [0.000_000_03, "2"]
            ]))
    );
}

#[tokio::test]
async fn executes_present_over_time_query_with_stepped_matrix_samples() {
    let response = api_stepped(
        &ERROR_ROWS,
        r#"present_over_time({app="api"} |= "error" [20ns])"#,
        STEPPED_PLAN,
    )
    .await;

    assert!(
        response
            == api_matrix(json!([
                [0.000_000_01, "1"],
                [0.000_000_02, "1"],
                [0.000_000_03, "1"]
            ]))
    );
}

#[tokio::test]
async fn executes_rate_query_with_stepped_matrix_samples() {
    let response = api_stepped(
        &ERROR_ROWS,
        r#"rate({app="api"} |= "error" [20s])"#,
        RATE_PLAN,
    )
    .await;

    assert!(
        response
            == api_matrix(json!([
                [0.000_000_01, "0.05"],
                [0.000_000_02, "0.1"],
                [0.000_000_03, "0.15"]
            ]))
    );
}

#[tokio::test]
async fn executes_bytes_over_time_query_with_stepped_matrix_samples() {
    let response = api_stepped(
        &GROWING_ROWS,
        r#"bytes_over_time({app="api"} [20ns])"#,
        STEPPED_PLAN,
    )
    .await;

    assert!(
        response
            == api_matrix(json!([
                [0.000_000_01, "2"],
                [0.000_000_02, "5"],
                [0.000_000_03, "7"]
            ]))
    );
}

#[tokio::test]
async fn executes_bytes_rate_query_with_stepped_matrix_samples() {
    let response = api_stepped(&GROWING_ROWS, r#"bytes_rate({app="api"} [20s])"#, RATE_PLAN).await;

    assert!(
        response
            == api_matrix(json!([
                [0.000_000_01, "0.1"],
                [0.000_000_02, "0.25"],
                [0.000_000_03, "0.45"]
            ]))
    );
}

/// One block of `span` whose rows `pod_rows` builds from the series of
/// prod pods `a` and `b`.
fn two_prod_pods(span: BlockSpan, pod_rows: impl FnOnce([u64; 2]) -> Vec<LogRow>) -> Fixture {
    let mut fixture = Fixture::new();
    let pods = prod_pods(&mut fixture, ["a", "b"]);
    fixture.block(PartitionIndex(0), span, pod_rows(pods));
    fixture
}

#[tokio::test]
async fn executes_sum_by_vector_aggregation_with_stepped_matrix_samples() {
    let fixture = two_prod_pods(
        BlockSpan {
            first: 10,
            last: 29,
        },
        |[pod_a, pod_b]| {
            vec![
                row(pod_a, log_entry(10, "api error one")),
                row(pod_b, log_entry(19, "api error two")),
                row(pod_a, log_entry(29, "api error three")),
            ]
        },
    );

    let response = fixture
        .metric_range(&RangeQuery {
            query: r#"sum by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            plan: STEPPED_PLAN,
            eval: TimeRange {
                start_ns: 10,
                end_ns: 30,
            },
            step_ns: 10,
        })
        .await;

    assert!(
        response
            == env_matrix(json!([
                [0.000_000_01, "1"],
                [0.000_000_02, "2"],
                [0.000_000_03, "2"]
            ]))
    );
}

#[tokio::test]
async fn executes_avg_without_vector_aggregation_with_stepped_matrix_samples() {
    let fixture = two_prod_pods(
        BlockSpan {
            first: 10,
            last: 29,
        },
        |[pod_a, pod_b]| {
            vec![
                row(pod_a, log_entry(10, "aa")),
                row(pod_b, log_entry(19, "bbbb")),
                row(pod_a, log_entry(29, "cccccc")),
            ]
        },
    );

    let response = fixture
        .metric_range(&RangeQuery {
            query: r#"avg without (pod) (bytes_over_time({app="api", env="prod"} [20ns]))"#,
            plan: STEPPED_PLAN,
            eval: TimeRange {
                start_ns: 10,
                end_ns: 30,
            },
            step_ns: 10,
        })
        .await;

    assert!(
        response
            == api_matrix(json!([
                [0.000_000_01, "2"],
                [0.000_000_02, "3"],
                [0.000_000_03, "5"]
            ]))
    );
}

#[tokio::test]
async fn executes_count_min_and_max_vector_aggregations() {
    let fixture = two_prod_pods(
        BlockSpan {
            first: 10,
            last: 19,
        },
        |[pod_a, pod_b]| {
            vec![
                row(pod_a, log_entry(10, "api error one")),
                row(pod_a, log_entry(19, "api error two")),
                row(pod_b, log_entry(19, "api error three")),
            ]
        },
    );

    let cases = [
        (
            r#"count by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            json!([
                [0.000_000_01, "1"],
                [0.000_000_02, "2"],
                [0.000_000_03, "2"]
            ]),
        ),
        (
            r#"min by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            json!([
                [0.000_000_01, "1"],
                [0.000_000_02, "1"],
                [0.000_000_03, "1"]
            ]),
        ),
        (
            r#"max by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            json!([
                [0.000_000_01, "1"],
                [0.000_000_02, "2"],
                [0.000_000_03, "1"]
            ]),
        ),
    ];

    for (query, expected_values) in cases {
        let response = fixture
            .metric_range(&RangeQuery {
                query,
                plan: STEPPED_PLAN,
                eval: TimeRange {
                    start_ns: 10,
                    end_ns: 30,
                },
                step_ns: 10,
            })
            .await;

        assert!(response == env_matrix(expected_values));
    }
}

fn three_prod_pods_with_one_two_three_errors() -> Fixture {
    let mut fixture = Fixture::new();
    let [pod_a, pod_b, pod_c] = prod_pods(&mut fixture, ["a", "b", "c"]);
    fixture.block(
        PartitionIndex(0),
        BlockSpan {
            first: 10,
            last: 29,
        },
        vec![
            row(pod_a, log_entry(11, "api error one")),
            row(pod_b, log_entry(12, "api error two")),
            row(pod_b, log_entry(13, "api error three")),
            row(pod_c, log_entry(14, "api error four")),
            row(pod_c, log_entry(15, "api error five")),
            row(pod_c, log_entry(16, "api error six")),
        ],
    );
    fixture
}

#[tokio::test]
async fn executes_count_values_vector_aggregation() {
    let fixture = three_prod_pods_with_one_two_three_errors();

    let response = fixture
        .metric_range(&RangeQuery {
 query: r#"count_values by (env) ("events", count_over_time({app="api"} |= "error" [20ns]))"#,
 plan: STEPPED_PLAN,
 eval: TimeRange { start_ns: 30, end_ns: 30 },
 step_ns: 10,
 })
        .await;

    assert!(
        response
            == matrix(json!([
                {
                    "metric": {
                        "env": "prod",
                        "events": "1"
                    },
                    "values": [[0.000_000_03, "1"]]
                },
                {
                    "metric": {
                        "env": "prod",
                        "events": "2"
                    },
                    "values": [[0.000_000_03, "1"]]
                },
                {
                    "metric": {
                        "env": "prod",
                        "events": "3"
                    },
                    "values": [[0.000_000_03, "1"]]
                }
            ]))
    );
}

#[tokio::test]
async fn executes_stdvar_and_stddev_vector_aggregations() {
    let fixture = three_prod_pods_with_one_two_three_errors();

    let cases = [
        (
            r#"stdvar by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            "0.666666666",
        ),
        (
            r#"stddev by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            "0.81649658",
        ),
    ];

    for (query, expected_value) in cases {
        let response = fixture
            .metric_range(&RangeQuery {
                query,
                plan: STEPPED_PLAN,
                eval: TimeRange {
                    start_ns: 30,
                    end_ns: 30,
                },
                step_ns: 10,
            })
            .await;

        assert!(response == env_matrix(json!([[0.000_000_03, expected_value]])));
    }
}

fn pod_matrix(samples: &[PodSample<'_>]) -> Value {
    matrix(Value::Array(
        samples
            .iter()
            .map(
                |&PodSample {
                     env,
                     pod,
                     sample_value,
                 }| {
                    json!({
                        "metric": {
                            "app": "api",
                            "env": env,
                            "pod": pod
                        },
                        "values": [
                            [0.000_000_03, sample_value]
                        ]
                    })
                },
            )
            .collect(),
    ))
}

#[tokio::test]
async fn executes_topk_and_bottomk_vector_aggregations() {
    let mut fixture = Fixture::new();
    let [api_a, api_b] = prod_pods(&mut fixture, ["a", "b"]);
    let worker_a = fixture.series(labels([("app", "api"), ("env", "stage"), ("pod", "a")]));
    let worker_b = fixture.series(labels([("app", "api"), ("env", "stage"), ("pod", "b")]));
    fixture.block(
        PartitionIndex(0),
        BlockSpan {
            first: 10,
            last: 29,
        },
        vec![
            row(api_a, log_entry(10, "api error a1")),
            row(api_a, log_entry(11, "api error a2")),
            row(api_b, log_entry(12, "api error b1")),
            row(api_b, log_entry(13, "api error b2")),
            row(api_b, log_entry(14, "api error b3")),
            row(worker_a, log_entry(15, "api error c1")),
            row(worker_b, log_entry(16, "api error d1")),
            row(worker_b, log_entry(17, "api error d2")),
        ],
    );

    let cases = [
        (
            r#"topk by (env) (1, count_over_time({app="api"} |= "error" [30ns]))"#,
            pod_matrix(&[
                PodSample {
                    env: "prod",
                    pod: "b",
                    sample_value: "3",
                },
                PodSample {
                    env: "stage",
                    pod: "b",
                    sample_value: "2",
                },
            ]),
        ),
        (
            r#"approx_topk(2, count_over_time({app="api"} |= "error" [30ns]))"#,
            pod_matrix(&[
                PodSample {
                    env: "prod",
                    pod: "a",
                    sample_value: "2",
                },
                PodSample {
                    env: "prod",
                    pod: "b",
                    sample_value: "3",
                },
            ]),
        ),
        (
            r#"bottomk(2, count_over_time({app="api"} |= "error" [30ns]))"#,
            pod_matrix(&[
                PodSample {
                    env: "prod",
                    pod: "a",
                    sample_value: "2",
                },
                PodSample {
                    env: "stage",
                    pod: "a",
                    sample_value: "1",
                },
            ]),
        ),
    ];

    for (query, expected) in cases {
        let response = fixture
            .metric_range(&RangeQuery {
                query,
                plan: WHOLE_FIXTURE,
                eval: TimeRange {
                    start_ns: 30,
                    end_ns: 30,
                },
                step_ns: 1,
            })
            .await;

        assert!(response == expected);
    }
}

#[tokio::test]
async fn executes_sort_and_sort_desc_vector_aggregations() {
    let mut fixture = Fixture::new();
    let [pod_a, pod_b, pod_c] = prod_pods(&mut fixture, ["a", "b", "c"]);
    fixture.block(
        PartitionIndex(0),
        BlockSpan {
            first: 10,
            last: 29,
        },
        vec![
            row(pod_a, log_entry(10, "api error a1")),
            row(pod_a, log_entry(11, "api error a2")),
            row(pod_b, log_entry(12, "api error b1")),
            row(pod_c, log_entry(13, "api error c1")),
            row(pod_c, log_entry(14, "api error c2")),
            row(pod_c, log_entry(15, "api error c3")),
        ],
    );

    for (query, expected_pods) in [
        (
            r#"sort(count_over_time({app="api"} |= "error" [30ns]))"#,
            ["b", "a", "c"],
        ),
        (
            r#"sort_desc(count_over_time({app="api"} |= "error" [30ns]))"#,
            ["c", "a", "b"],
        ),
    ] {
        let response = fixture
            .metric_range(&RangeQuery {
                query,
                plan: WHOLE_FIXTURE,
                eval: TimeRange {
                    start_ns: 30,
                    end_ns: 30,
                },
                step_ns: 1,
            })
            .await;

        let pods = response["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|series| series["metric"]["pod"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(pods == expected_pods);
    }
}

#[tokio::test]
async fn empty_stream_plan_returns_empty_loki_streams_result() {
    let response = Fixture::new().stream(r#"{app="api"}"#).await;

    assert!(response == streams(json!([])));
}

#[tokio::test]
async fn executes_stream_query_over_object_store_blocks_as_loki_json() {
    let mut fixture = Fixture::new();
    let api = fixture.series(labels(API_PROD));
    let worker = fixture.series(labels([("app", "worker"), ("env", "prod")]));
    fixture
        .object_block(
            PartitionIndex(0),
            BlockSpan {
                first: 10,
                last: 19,
            },
            rows(api, &[log_entry(10, "api ok"), log_entry(19, "api error")]),
        )
        .await;
    fixture
        .object_block(
            PartitionIndex(1),
            BlockSpan {
                first: 20,
                last: 29,
            },
            rows(worker, &[log_entry(25, "worker error")]),
        )
        .await;

    let response = fixture
        .object_store_stream(r#"{app="api"} |= "error""#)
        .await;

    assert!(response == api_stream(json!([["19", "api error"]])));
}

async fn object_store_with_missing_block(entries: &[LogEntry<'_>]) -> Fixture {
    let mut fixture = Fixture::new();
    let api = fixture.series(labels(API_PROD));
    fixture
        .object_block(
            PartitionIndex(0),
            BlockSpan {
                first: 10,
                last: 19,
            },
            rows(api, entries),
        )
        .await;
    fixture.missing_block(
        BlockSpan {
            first: 20,
            last: 29,
        },
        api,
    );
    fixture
}

#[tokio::test]
async fn object_store_stream_query_returns_partial_result_with_warning_for_unreadable_block() {
    let fixture = object_store_with_missing_block(&[log_entry(19, "api error")]).await;

    let response = fixture
        .object_store_stream(r#"{app="api"} |= "error""#)
        .await;

    assert!(response == with_warning(api_stream(json!([["19", "api error"]]))));
}

#[tokio::test]
async fn object_store_metric_query_returns_partial_result_with_warning_for_unreadable_block() {
    let fixture =
        object_store_with_missing_block(&[log_entry(10, "api ok"), log_entry(19, "api error")])
            .await;
    let (plan, query) = fixture.metric_plan(
        WHOLE_FIXTURE,
        r#"count_over_time({app="api"} |= "error" [30ns])"#,
    );

    let response = execute_metric_query_from_object_store(
        Arc::clone(&fixture.store),
        &fixture.prefix,
        &plan,
        &query,
        &fixture.label_index,
    )
    .await
    .unwrap();

    assert!(response == with_warning(api_matrix_at_30("1")));
}

struct RecordingWalConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
}

impl RecordingWalConsumer {
    fn new(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl LogWalConsumer for RecordingWalConsumer {
    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        Ok(())
    }
}

fn kafka_header(key: &str, value: &str) -> KafkaWalHeader {
    KafkaWalHeader {
        key: key.to_string(),
        value: Some(value.as_bytes().to_vec()),
    }
}
