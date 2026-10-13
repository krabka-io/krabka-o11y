//! Queriers built over an object store, and the tenant manifest each request reads.

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use assert2::assert;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LabelPair, LogBlockIndex as BlockIndex, LogLabels,
    LogRow, TimeRange, log_block_object_path, write_log_block, write_log_block_to_object_store,
    write_log_index_manifest, write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_to_object_store, write_tenant_log_index_shards_to_object_store,
};
use krabka_observability::{
    InMemoryWalSink, LogWalSink, QuerierIndexSource, QuerierState, Role, ServiceConfig,
    ServiceDependencies, WalLogRecord, build_service_router, loki_router,
};
use krabka_units::convert::ByteSizeExt as _;
use object_store::{ObjectStoreExt as _, local::LocalFileSystem, path::Path as ObjectPath};
use serde_json::{Value, json};
use support::{
    BlockSpan, LogEntry, LokiStatsCounts, LokiSuccess, Tenant,
    expected_loki_forwarded_api_error as expected_api_error, json_body, log_entry,
    loki_forwarded_tenant_object_store_shard_catalog_service_fixture, send,
    tenant_object_store_shard_catalog_config_fixture,
};

fn log_labels(pairs: impl IntoIterator<Item = LabelPair>) -> LogLabels {
    let mut labels = LogLabels::new();
    for pair in pairs {
        labels.insert(pair.name, pair.value);
    }
    labels
}

fn api_prod_labels() -> LogLabels {
    log_labels([LabelPair::new("app", "api"), LabelPair::new("env", "prod")])
}

fn api_stage_labels() -> LogLabels {
    log_labels([LabelPair::new("app", "api"), LabelPair::new("env", "stage")])
}

struct IndexedSource {
    labels: LogLabels,
    fingerprint: u64,
}

struct QueryLimitCase {
    limit: usize,
    end: i64,
    interval: Option<i64>,
    expected_rows: usize,
}

#[derive(Clone)]
struct MetadataEntry {
    line: &'static str,
    user: &'static str,
}

struct TimedEntries {
    timestamp: i64,
    entries: Vec<MetadataEntry>,
}

struct UriResponseCase {
    uri: &'static str,
    expected: Value,
}

const ERROR_QUERY_RANGE: &str = "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=30.000000000";
const MISSING_BLOCK_WARNING: &str =
    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet";
const SECONDS: i64 = 1_000_000_000;

async fn assert_query_range_reads_api_error(state: QuerierState) {
    let response = Tenant("tenant-a")
        .get_json(&loki_router(state), ERROR_QUERY_RANGE)
        .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(body == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_indexes_from_persisted_manifest() {
    assert_query_range_reads_api_error(persisted_fixture()).await;
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_manifest() {
    assert_query_range_reads_api_error(tenant_object_store_fixture().await).await;
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_shard() {
    assert_query_range_reads_api_error(tenant_object_store_shard_fixture().await).await;
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_shard_catalog() {
    assert_query_range_reads_api_error(tenant_object_store_shard_catalog_fixture().await).await;
}

#[tokio::test]
async fn query_endpoint_can_build_querier_from_object_store_shard_catalog_config() {
    let state = tenant_object_store_shard_catalog_config_fixture()
        .await
        .state;
    assert_query_range_reads_api_error(state).await;
}

/// A querier over a tenant manifest in an object store, with a hot tail of
/// the same rows, and one cold block fetch at a time.
/// A querier over a tenant-a manifest in a local object store, and a hot
/// tail.
struct HotAndColdQuerier<'a> {
    object_dir: &'a Path,
    data_root: &'a Path,
    prefix: &'a ObjectPath,
    /// The indexes the manifest is written from.
    label_index: &'a LabelIndex,
    block_index: &'a BlockIndex,
    /// The hot tail's lines, each at 30 ns.
    hot_lines: Vec<HotLine<'a>>,
}

async fn hot_and_cold_router(querier: HotAndColdQuerier<'_>) -> Router {
    let HotAndColdQuerier {
        object_dir,
        data_root,
        prefix,
        label_index,
        block_index,
        hot_lines,
    } = querier;
    let hot_tail = hot_tail_at_30ns(hot_lines).await;
    write_tenant_log_index_manifest_to_object_store(
        &LocalFileSystem::new_with_prefix(object_dir).unwrap(),
        prefix,
        "tenant-a",
        label_index,
        block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        object_store_url: Some(format!("file://{}", object_dir.display())),
        data_root: data_root.into(),
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".into()),
        index_prefix: Some(prefix.to_string()),
        querier_cold_block_fetch_concurrency: std::num::NonZeroUsize::MIN,
        ..ServiceConfig::default()
    };
    build_service_router(
        &config,
        ServiceDependencies::default().with_hot_tail(hot_tail, 0),
        None,
    )
    .await
    .unwrap()
}

/// A tenant-a hot line at 30 ns, with its `user` structured metadata.
struct HotLine<'a> {
    source: &'a LogLabels,
    line: &'a str,
    user: &'a str,
}

/// A hot tail that holds `hot_lines`, each at 30 ns.
async fn hot_tail_at_30ns<'a>(hot_lines: impl IntoIterator<Item = HotLine<'a>>) -> InMemoryWalSink {
    let hot_tail = InMemoryWalSink::default();
    for hot_line in hot_lines {
        hot_tail
            .append(WalLogRecord {
                tenant: "tenant-a".into(),
                labels: hot_line.source.clone(),
                timestamp_ns: 30,
                line: hot_line.line.into(),
                structured_metadata: log_labels([LabelPair::new("user", hot_line.user)]),
                position: None,
            })
            .await
            .unwrap();
    }
    hot_tail
}

/// How a query response encodes stream labels and structured metadata.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LabelEncoding {
    /// Structured metadata folded into the stream labels.
    Flat,
    /// Structured metadata beside each entry, as
    /// `X-Loki-Response-Encoding-Flags: categorize-labels` asks.
    Categorized,
}

async fn query_streams(app: &Router, uri: &str, encoding: LabelEncoding) -> Value {
    let response = send(
        app,
        Request::builder()
            .uri(uri)
            .header("X-Scope-OrgID", "tenant-a")
            .header(
                "X-Loki-Response-Encoding-Flags",
                match encoding {
                    LabelEncoding::Categorized => "categorize-labels",
                    LabelEncoding::Flat => "",
                },
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(response.status() == StatusCode::OK);
    json_body(response).await["data"]["result"].clone()
}

#[tokio::test]
async fn overlapping_hot_and_cold_rows_do_not_spend_the_query_limit_twice() {
    let object_dir = tempfile::tempdir().unwrap();
    let data_root = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(object_dir.path()).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let sources = ["a", "b"].map(|replica| {
        let labels = log_labels([
            LabelPair::new("app", "api"),
            LabelPair::new("replica", replica),
        ]);
        let fingerprint = label_index.insert_series("tenant-a", labels.clone());
        IndexedSource {
            labels,
            fingerprint,
        }
    });
    let mut block_index = BlockIndex::default();
    for span in [
        BlockSpan {
            first: 10,
            last: 20,
        },
        BlockSpan {
            first: 30,
            last: 30,
        },
    ] {
        let rows = sources
            .iter()
            .flat_map(|source| {
                [10, 20, 30]
                    .into_iter()
                    .filter(move |timestamp| (span.first..=span.last).contains(timestamp))
                    .map(|timestamp| {
                        LogRow::new(
                            source.fingerprint,
                            timestamp,
                            format!("line-{timestamp}"),
                            log_labels([LabelPair::new("user", "alice")]),
                        )
                    })
            })
            .collect();
        block_index.insert(
            write_log_block_to_object_store(
                &store,
                &prefix,
                &block_key(
                    "tenant-a",
                    BlockSpan {
                        first: span.first,
                        last: span.last,
                    },
                ),
                rows,
            )
            .await
            .unwrap(),
        );
    }
    let app = hot_and_cold_router(HotAndColdQuerier {
        object_dir: object_dir.path(),
        data_root: data_root.path(),
        prefix: &prefix,
        label_index: &label_index,
        block_index: &block_index,
        hot_lines: sources
            .iter()
            .map(|source| HotLine {
                source: &source.labels,
                line: "line-30",
                user: "alice",
            })
            .collect(),
    })
    .await;
    for encoding in [LabelEncoding::Flat, LabelEncoding::Categorized] {
        let categorized = encoding == LabelEncoding::Categorized;
        for direction in ["forward", "backward"] {
            for case in [
                QueryLimitCase {
                    limit: 2,
                    end: 40,
                    interval: None,
                    expected_rows: 1,
                },
                QueryLimitCase {
                    limit: 4,
                    end: 40,
                    interval: None,
                    expected_rows: 2,
                },
                QueryLimitCase {
                    limit: 6,
                    end: 40,
                    interval: None,
                    expected_rows: 3,
                },
                QueryLimitCase {
                    limit: 4,
                    end: 30,
                    interval: None,
                    expected_rows: 2,
                },
                QueryLimitCase {
                    limit: 4,
                    end: 40,
                    interval: Some(15),
                    expected_rows: 2,
                },
            ] {
                let interval_query = case
                    .interval
                    .map(|interval| format!("&interval=0.{interval:09}"))
                    .unwrap_or_default();
                let result = query_streams(&app, &format!(
                        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.{}&direction={direction}&limit={}{}",
                        format!("{:09}", case.end), case.limit, interval_query
                    ), encoding)
                .await;
                let expected = sources.iter().map(|source| {
                    let mut stream = source.labels.clone();
                    if !categorized { stream.insert("user".into(), "alice".into()); }
                    let mut timestamps = if case.end == 30 {
                        vec![10, 20]
                    } else if case.interval.is_some() {
                        vec![10, 30]
                    } else {
                        vec![10, 20, 30]
                    };
                    if direction == "backward" { timestamps.reverse(); }
                    let values = timestamps.into_iter().take(case.expected_rows).map(|timestamp| {
                        if categorized {
                            json!([timestamp.to_string(), format!("line-{timestamp}"), {"structuredMetadata": {"user": "alice"}}])
                        } else {
                            json!([timestamp.to_string(), format!("line-{timestamp}")])
                        }
                    }).collect::<Vec<_>>();
                    json!({"stream": stream, "values": values})
                }).collect::<Vec<_>>();
                assert!(result == json!(expected));
            }
        }
    }
}

#[tokio::test]
async fn nonadjacent_hot_and_cold_rows_keep_unique_entries_and_metadata() {
    for distinct_metadata in [false, true] {
        let object_dir = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(object_dir.path()).unwrap();
        let prefix = ObjectPath::from("indexes");
        let source = log_labels([LabelPair::new("app", "api")]);
        let mut label_index = LabelIndex::default();
        let fingerprint = label_index.insert_series("tenant-a", source.clone());
        let mut newest_rows = vec![
            MetadataEntry {
                line: "A",
                user: "alice",
            },
            MetadataEntry {
                line: "B",
                user: "alice",
            },
        ];
        if distinct_metadata {
            newest_rows.push(MetadataEntry {
                line: "A",
                user: "bob",
            });
        }
        let mut block_index = BlockIndex::default();
        for batch in [
            TimedEntries {
                timestamp: 10,
                entries: vec![MetadataEntry {
                    line: "old",
                    user: "alice",
                }],
            },
            TimedEntries {
                timestamp: 30,
                entries: newest_rows.clone(),
            },
        ] {
            block_index.insert(
                write_log_block_to_object_store(
                    &store,
                    &prefix,
                    &block_key(
                        "tenant-a",
                        BlockSpan {
                            first: batch.timestamp,
                            last: batch.timestamp,
                        },
                    ),
                    batch
                        .entries
                        .into_iter()
                        .map(|entry| {
                            LogRow::new(
                                fingerprint,
                                batch.timestamp,
                                entry.line,
                                log_labels([LabelPair::new("user", entry.user)]),
                            )
                        })
                        .collect(),
                )
                .await
                .unwrap(),
            );
        }
        let app = hot_and_cold_router(HotAndColdQuerier {
            object_dir: object_dir.path(),
            data_root: data_root.path(),
            prefix: &prefix,
            label_index: &label_index,
            block_index: &block_index,
            hot_lines: newest_rows
                .into_iter()
                .map(|entry| HotLine {
                    source: &source,
                    line: entry.line,
                    user: entry.user,
                })
                .collect(),
        })
        .await;
        for encoding in [LabelEncoding::Flat, LabelEncoding::Categorized] {
            let categorized = encoding == LabelEncoding::Categorized;
            for direction in ["forward", "backward"] {
                let limit = if distinct_metadata || direction == "forward" {
                    4
                } else {
                    3
                };
                let actual = query_streams(&app, &format!("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000040&direction={direction}&limit={limit}"), encoding)
                .await;
                let expected = if categorized {
                    let mut values = vec![
                        json!(["30", "B", {"structuredMetadata": {"user": "alice"}}]),
                        json!(["30", "A", {"structuredMetadata": {"user": "alice"}}]),
                        json!(["10", "old", {"structuredMetadata": {"user": "alice"}}]),
                    ];
                    if distinct_metadata {
                        values.insert(
                            0,
                            json!(["30", "A", {"structuredMetadata": {"user": "bob"}}]),
                        );
                    }
                    json!([{"stream": {"app": "api"}, "values": values}])
                } else {
                    let mut streams = vec![
                        json!({"stream": {"app": "api", "user": "alice"}, "values": [["30", "B"], ["30", "A"], ["10", "old"]]}),
                    ];
                    if distinct_metadata {
                        streams.push(
                        json!({"stream": {"app": "api", "user": "bob"}, "values": [["30", "A"]]}),
                    );
                    }
                    json!(streams)
                };
                let normalize_ties = |mut streams: serde_json::Value| {
                    for stream in streams.as_array_mut().unwrap() {
                        let values = stream["values"].as_array_mut().unwrap();
                        // Compare full entries at equal times without changing timestamp order.
                        for tied in values.chunk_by_mut(|left, right| left[0] == right[0]) {
                            tied.sort_by_cached_key(serde_json::Value::to_string);
                        }
                    }
                    streams
                };
                let expected = if direction == "forward" {
                    let mut expected = expected;
                    for stream in expected.as_array_mut().unwrap() {
                        stream["values"].as_array_mut().unwrap().reverse();
                    }
                    expected
                } else {
                    expected
                };
                assert!(normalize_ties(actual) == normalize_ties(expected));
            }
        }
    }
}

fn block_key(tenant: &str, span: BlockSpan) -> BlockKey {
    BlockKey::new(
        tenant,
        0,
        span.first,
        span.last,
        TimeRange::new(span.first, span.last).unwrap(),
    )
}

/// Blocks and tenant indexes written to an object store in a kept directory,
/// which a configured querier then reads.
struct ObjectStoreIndex {
    object_dir: PathBuf,
    data_root: PathBuf,
    store: LocalFileSystem,
    prefix: ObjectPath,
    label_index: LabelIndex,
    block_index: BlockIndex,
}

impl ObjectStoreIndex {
    fn new() -> Self {
        let object_dir = tempfile::tempdir().unwrap().keep();
        let data_root = tempfile::tempdir().unwrap().keep();
        let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
        Self {
            object_dir,
            data_root,
            store,
            prefix: ObjectPath::from("indexes"),
            label_index: LabelIndex::default(),
            block_index: BlockIndex::default(),
        }
    }

    fn series(&mut self, tenant: &str, series_labels: LogLabels) -> u64 {
        self.label_index.insert_series(tenant, series_labels)
    }

    /// Writes one block to the object store and returns its size in bytes.
    async fn block(&mut self, key: &BlockKey, rows: Vec<LogRow>) -> u64 {
        let block = write_log_block_to_object_store(&self.store, &self.prefix, key, rows)
            .await
            .unwrap();
        let bytes = block.size.bytes_u64();
        self.block_index.insert(block);
        bytes
    }

    fn missing_block(&mut self, span: BlockSpan, series: u64) {
        self.block_index.insert(BlockDescriptor::new(
            block_key("tenant-a", span),
            BTreeSet::from([series]),
        ));
    }

    async fn write_manifest(&self, tenant: &str) {
        write_tenant_log_index_manifest_to_object_store(
            &self.store,
            &self.prefix,
            tenant,
            &self.label_index,
            &self.block_index,
        )
        .await
        .unwrap();
    }

    async fn write_shard(&self, tenant: &str, shard: TimeRange) {
        write_tenant_log_index_shards_to_object_store(
            &self.store,
            &self.prefix,
            tenant,
            &[shard],
            &self.label_index,
            &self.block_index,
        )
        .await
        .unwrap();
    }

    fn config(&self, tenant: Option<&str>, source: QuerierIndexSource) -> ServiceConfig {
        ServiceConfig {
            target: Role::Querier,
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            object_store_url: Some(format!("file://{}", self.object_dir.display())),
            wal_bootstrap_server: None,
            wal_topic: "__krabka_observability_logs_wal".to_string(),
            wal_group_id: "krabka-observability-querier-tail".to_string(),
            data_root: self.data_root.clone(),
            querier_index_source: source,
            tenant: tenant.map(str::to_string),
            index_prefix: Some(self.prefix.to_string()),
            query_start_ns: None,
            query_end_ns: None,
            max_query_range: None,
            max_query_series: None,
            max_query_read: None,
            max_query_string_bytes: None,
            max_ingest_body: None,
            wal_append_timeout: None,
            ..ServiceConfig::default()
        }
    }

    async fn router_with(
        &self,
        config: &ServiceConfig,
        dependencies: ServiceDependencies,
    ) -> Router {
        build_service_router(config, dependencies, None)
            .await
            .unwrap()
    }

    async fn router(&self, tenant: Option<&str>, source: QuerierIndexSource) -> Router {
        self.router_with(&self.config(tenant, source), ServiceDependencies::default())
            .await
    }
}

fn one_stream(env: &str, values: Value) -> Value {
    let mut stream = json!({ "stream": { "app": "api", "env": env } });
    stream["values"] = values;
    Value::Array(vec![stream])
}

fn with_missing_block_warning(mut response: Value) -> Value {
    response["warnings"] = json!([MISSING_BLOCK_WARNING]);
    response
}

/// A tenant-a manifest whose `api` block at 10-19 holds `lines`, and that also
/// names a block at 20-29 whose object was never written.
struct AppWithBlockBytes {
    app: Router,
    block_bytes: u64,
}

async fn manifest_with_missing_block(entries: &[LogEntry<'_>]) -> AppWithBlockBytes {
    let mut index = ObjectStoreIndex::new();
    let api = index.series("tenant-a", api_prod_labels());
    let readable_block_bytes = index
        .block(
            &block_key(
                "tenant-a",
                BlockSpan {
                    first: 10,
                    last: 19,
                },
            ),
            entries
                .iter()
                .map(|entry| LogRow::new(api, entry.timestamp_ns, entry.line, BTreeMap::new()))
                .collect(),
        )
        .await;
    index.missing_block(
        BlockSpan {
            first: 20,
            last: 29,
        },
        api,
    );
    index.write_manifest("tenant-a").await;
    let app = index
        .router(
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
        )
        .await;
    AppWithBlockBytes {
        app,
        block_bytes: readable_block_bytes,
    }
}

#[tokio::test]
async fn configured_object_store_query_returns_partial_warning_for_missing_block() {
    let fixture = manifest_with_missing_block(&[log_entry(19, "api error")]).await;
    let app = fixture.app;
    let readable_block_bytes = fixture.block_bytes;

    let response = Tenant("tenant-a").get_json(&app, ERROR_QUERY_RANGE).await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == with_missing_block_warning(
            LokiSuccess {
                result_type: "streams",
                data_result: one_stream("prod", json!([["19", "api error"]])),
                stats: LokiStatsCounts {
                    store_bytes: readable_block_bytes,
                    store_lines: 1,
                    chunks: 2,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
        )
    );
}

#[tokio::test]
async fn configured_object_store_backward_limited_query_stops_after_newest_block() {
    let mut index = ObjectStoreIndex::new();
    let api = index.series("tenant-a", api_prod_labels());
    index.missing_block(
        BlockSpan {
            first: 10,
            last: 19,
        },
        api,
    );
    let newest_block_bytes = index
        .block(
            &block_key(
                "tenant-a",
                BlockSpan {
                    first: 20,
                    last: 29,
                },
            ),
            vec![
                LogRow::new(api, 20, "api older error", BTreeMap::new()),
                LogRow::new(api, 29, "api newest error", BTreeMap::new()),
            ],
        )
        .await;
    index.write_manifest("tenant-a").await;
    let app = index
        .router(
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
        )
        .await;

    let response = Tenant("tenant-a").get_json(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030&direction=backward&limit=1")
    .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == LokiSuccess {
            result_type: "streams",
            data_result: one_stream("prod", json!([["29", "api newest error"]])),
            stats: LokiStatsCounts {
                store_bytes: newest_block_bytes,
                store_lines: 1,
                chunks: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats(),
        }
        .json()
    );
}

#[tokio::test]
async fn configured_object_store_query_merges_hot_tail_with_source_split_stats() {
    let mut index = ObjectStoreIndex::new();
    let api = index.series("tenant-a", api_prod_labels());
    let cold_block_bytes = index
        .block(
            &block_key(
                "tenant-a",
                BlockSpan {
                    first: 10,
                    last: 19,
                },
            ),
            vec![LogRow::new(api, 19, "api cold error", BTreeMap::new())],
        )
        .await;
    index.write_manifest("tenant-a").await;

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: api_prod_labels(),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();

    let config = ServiceConfig {
        wal_group_id: "krabka-observability-querier-object-hot-tail".to_string(),
        ..index.config(
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
        )
    };
    let app = index
        .router_with(
            &config,
            ServiceDependencies::default().with_hot_tail(hot_tail, 19),
        )
        .await;

    let response = Tenant("tenant-a")
        .get_json(&app, &format!("{ERROR_QUERY_RANGE}&direction=forward"))
        .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == LokiSuccess {
            result_type: "streams",
            data_result: one_stream(
                "prod",
                json!([["19", "api cold error"], ["20", "api hot error"]])
            ),
            stats: LokiStatsCounts {
                store_bytes: cold_block_bytes,
                store_lines: 1,
                ingester_lines: 1,
                chunks: 1
            }
            .expected_stats(),
        }
        .json()
    );
}

#[tokio::test]
async fn configured_object_store_metric_query_returns_partial_warning_for_missing_block() {
    let fixture =
        manifest_with_missing_block(&[log_entry(10, "api ok"), log_entry(19, "api error")]).await;
    let app = fixture.app;
    let readable_block_bytes = fixture.block_bytes;

    let response = Tenant("tenant-a").get_json(&app, "/loki/api/v1/query_range?query=count_over_time(%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D)&start=0.000000030&end=0.000000030&step=1ns")
    .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == with_missing_block_warning(
            LokiSuccess {
                result_type: "matrix",
                data_result: json!([{
                    "metric": { "app": "api", "env": "prod" },
                    "values": [[0.000_000_03, "1"]]
                }]),
                stats: LokiStatsCounts {
                    store_bytes: readable_block_bytes,
                    store_lines: 1,
                    chunks: 2,
                    ..LokiStatsCounts::default()
                }
                .expected_stats(),
            }
            .json()
        )
    );
}

#[tokio::test]
async fn configured_object_store_index_stats_endpoint_counts_entries_from_object_store_blocks() {
    let mut index = ObjectStoreIndex::new();
    let api = index.series("tenant-a", api_prod_labels());
    let expected_block_bytes = index
        .block(
            &block_key(
                "tenant-a",
                BlockSpan {
                    first: 10,
                    last: 19,
                },
            ),
            vec![
                LogRow::new(api, 10, "api ok", BTreeMap::new()),
                LogRow::new(api, 19, "api error", BTreeMap::new()),
            ],
        )
        .await;
    index.write_manifest("tenant-a").await;
    let app = index
        .router(
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
        )
        .await;

    let response = Tenant("tenant-a").get_json(&app, "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019")
    .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == json!({
            "streams": 1,
            "chunks": 1,
            "entries": 2,
            "bytes": expected_block_bytes,
        })
    );
}

/// A manifest for tenant-b alone, with one `api` row at 29, served by a
/// querier that takes the tenant from each request.
async fn tenant_b_manifest(line: &str, metadata: BTreeMap<String, String>) -> AppWithBlockBytes {
    let mut index = ObjectStoreIndex::new();
    let tenant_b_api = index.series("tenant-b", api_stage_labels());
    let bytes = index
        .block(
            &block_key(
                "tenant-b",
                BlockSpan {
                    first: 20,
                    last: 29,
                },
            ),
            vec![LogRow::new(tenant_b_api, 29, line, metadata)],
        )
        .await;
    index.write_manifest("tenant-b").await;
    let app = index
        .router(None, QuerierIndexSource::TenantObjectStoreManifest)
        .await;
    AppWithBlockBytes {
        app,
        block_bytes: bytes,
    }
}

#[tokio::test]
async fn configured_object_store_index_stats_endpoint_loads_request_tenant_manifest() {
    let fixture = tenant_b_manifest("tenant-b api error", BTreeMap::new()).await;
    let app = fixture.app;
    let expected_block_bytes = fixture.block_bytes;

    let response = Tenant("tenant-b").get_json(&app, "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000020&end=0.000000029")
    .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == json!({
            "streams": 1,
            "chunks": 1,
            "entries": 1,
            "bytes": expected_block_bytes,
        })
    );
}

#[tokio::test]
async fn configured_object_store_index_volume_endpoint_loads_request_tenant_manifest() {
    let fixture = tenant_b_manifest("tenant-b api error", BTreeMap::new()).await;
    let app = fixture.app;
    let expected_block_bytes = fixture.block_bytes;

    let response = Tenant("tenant-b").get_json(&app, "/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0.000000020&end=0.000000029")
    .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == LokiSuccess {
            result_type: "vector",
            data_result: json!([{
                "metric": { "app": "api", "env": "stage" },
                "value": [0.000_000_029, expected_block_bytes.to_string()]
            }]),
            stats: LokiStatsCounts {
                store_bytes: expected_block_bytes,
                store_lines: 0,
                chunks: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats(),
        }
        .json()
    );
}

#[tokio::test]
async fn configured_object_store_patterns_endpoint_loads_request_tenant_manifest() {
    let mut index = ObjectStoreIndex::new();
    let tenant_b_api = index.series("tenant-b", api_stage_labels());
    index
        .block(
            &block_key(
                "tenant-b",
                BlockSpan {
                    first: 100_000_000,
                    last: 1_100_000_000,
                },
            ),
            vec![
                LogRow::new(
                    tenant_b_api,
                    100_000_000,
                    "status=500 user=123 route=/checkout",
                    BTreeMap::new(),
                ),
                LogRow::new(
                    tenant_b_api,
                    1_100_000_000,
                    "status=503 user=456 route=/checkout",
                    BTreeMap::new(),
                ),
            ],
        )
        .await;
    index.write_manifest("tenant-b").await;
    let app = index
        .router(None, QuerierIndexSource::TenantObjectStoreManifest)
        .await;

    let response = Tenant("tenant-b")
        .get_json(
            &app,
            "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2.0&step=1s",
        )
        .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == json!({
            "status": "success",
            "data": [
                {
                    "pattern": "status=<_> user=<_> route=/checkout",
                    "samples": [
                        [0, 1],
                        [1, 1]
                    ]
                }
            ]
        })
    );
}

#[tokio::test]
async fn configured_object_store_detected_fields_endpoint_loads_request_tenant_manifest() {
    let fixture = tenant_b_manifest(
        r#"{"status":500}"#,
        log_labels([LabelPair::new("trace_id", "abc")]),
    )
    .await;
    let app = fixture.app;

    let response = Tenant("tenant-b").get_json(&app, "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0.000000020&end=0.000000029&limit=10")
    .await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == json!({
            "fields": [
                {
                    "label": "status",
                    "type": "int",
                    "cardinality": 1,
                    "parsers": ["json"],
                    "jsonPath": ["status"]
                },
                {
                    "label": "trace_id",
                    "type": "string",
                    "cardinality": 1,
                    "parsers": null
                }
            ],
            "limit": 10
        })
    );
}

#[tokio::test]
async fn configured_object_store_querier_loads_manifest_for_request_tenant_header() {
    let mut index = ObjectStoreIndex::new();
    let prod_api = index.series("tenant-a", api_prod_labels());
    let stage_api = index.series("tenant-b", api_stage_labels());
    index
        .block(
            &block_key(
                "tenant-a",
                BlockSpan {
                    first: 10,
                    last: 19,
                },
            ),
            vec![LogRow::new(
                prod_api,
                19,
                "tenant-a api error",
                BTreeMap::new(),
            )],
        )
        .await;
    let stage_block_bytes = index
        .block(
            &block_key(
                "tenant-b",
                BlockSpan {
                    first: 20 * SECONDS,
                    last: 29 * SECONDS,
                },
            ),
            vec![LogRow::new(
                stage_api,
                29 * SECONDS,
                "tenant-b api error",
                BTreeMap::new(),
            )],
        )
        .await;
    index.write_manifest("tenant-a").await;
    index.write_manifest("tenant-b").await;
    let app = index
        .router(None, QuerierIndexSource::TenantObjectStoreManifest)
        .await;

    assert_tenant_b_reads_stage_error(&app, stage_block_bytes).await;
}

async fn assert_tenant_b_reads_stage_error(app: &Router, stage_block_bytes: u64) {
    let response = Tenant("tenant-b").get_json(app, ERROR_QUERY_RANGE).await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(
        body == LokiSuccess {
            result_type: "streams",
            data_result: one_stream("stage", json!([["29000000000", "tenant-b api error"]])),
            stats: LokiStatsCounts {
                store_bytes: stage_block_bytes,
                store_lines: 1,
                chunks: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats(),
        }
        .json()
    );
}

async fn assert_tenant_b_labels(app: &Router, uri: &str) {
    let response = Tenant("tenant-b").get_json(app, uri).await;
    let status = response.status;
    let body = response.body;

    assert!(status == StatusCode::OK);
    assert!(body == json!({"status": "success", "data": ["app", "env"]}));
}

#[tokio::test]
async fn configured_object_store_labels_endpoint_loads_manifest_for_request_tenant_header() {
    let mut index = ObjectStoreIndex::new();
    index.series("tenant-b", api_stage_labels());
    index.write_manifest("tenant-b").await;
    let app = index
        .router(None, QuerierIndexSource::TenantObjectStoreManifest)
        .await;

    assert_tenant_b_labels(&app, "/loki/api/v1/labels").await;
}

#[tokio::test]
async fn configured_object_store_shard_catalog_querier_loads_shards_for_request_tenant_header() {
    let mut index = ObjectStoreIndex::new();
    let tenant_b_api = index.series("tenant-b", api_stage_labels());
    let tenant_b_block_bytes = index
        .block(
            &block_key(
                "tenant-b",
                BlockSpan {
                    first: 20 * SECONDS,
                    last: 29 * SECONDS,
                },
            ),
            vec![LogRow::new(
                tenant_b_api,
                29 * SECONDS,
                "tenant-b api error",
                BTreeMap::new(),
            )],
        )
        .await;
    index
        .write_shard(
            "tenant-b",
            TimeRange::new(20 * SECONDS, 29 * SECONDS).unwrap(),
        )
        .await;
    let app = index
        .router(None, QuerierIndexSource::TenantObjectStoreShards)
        .await;

    assert_tenant_b_reads_stage_error(&app, tenant_b_block_bytes).await;
}

#[tokio::test]
async fn configured_object_store_shard_catalog_labels_endpoint_loads_request_tenant_shards() {
    let mut index = ObjectStoreIndex::new();
    let tenant_b_api = index.series("tenant-b", api_stage_labels());
    index
        .block(
            &block_key(
                "tenant-b",
                BlockSpan {
                    first: 20,
                    last: 29,
                },
            ),
            vec![LogRow::new(
                tenant_b_api,
                29,
                "tenant-b api error",
                BTreeMap::new(),
            )],
        )
        .await;
    index
        .write_shard("tenant-b", TimeRange::new(20, 29).unwrap())
        .await;
    let app = index
        .router(None, QuerierIndexSource::TenantObjectStoreShards)
        .await;

    assert_tenant_b_labels(
        &app,
        "/loki/api/v1/labels?start=0.000000020&end=0.000000029",
    )
    .await;
}

/// Whether the indexes also name a `tenant-b` series.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TenantB {
    Absent,
    Present,
}

/// Writes the `api` block at 10-19 s to `dir`, with a `tenant-b` series
/// beside it when `tenant_b` says so, and returns the indexes that name it.
struct ApiSecondsIndexes {
    labels: LabelIndex,
    blocks: BlockIndex,
}

fn api_seconds_block(dir: &Path, tenant_b: TenantB) -> ApiSecondsIndexes {
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", api_prod_labels());
    if tenant_b == TenantB::Present {
        label_index.insert_series("tenant-b", api_prod_labels());
    }

    let api_block = write_log_block(
        dir,
        &block_key(
            "tenant-a",
            BlockSpan {
                first: 10 * SECONDS,
                last: 19 * SECONDS,
            },
        ),
        vec![
            LogRow::new(api, 10 * SECONDS, "api ok", BTreeMap::new()),
            LogRow::new(api, 19 * SECONDS, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    ApiSecondsIndexes {
        labels: label_index,
        blocks: block_index,
    }
}

fn persisted_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let indexes = api_seconds_block(&dir, TenantB::Absent);
    write_log_index_manifest(&dir, &indexes.labels, &indexes.blocks).unwrap();

    QuerierState::from_manifest(dir).unwrap()
}

async fn tenant_object_store_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let indexes = api_seconds_block(&dir, TenantB::Present);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &indexes.labels,
        &indexes.blocks,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store(dir, &store, &prefix, "tenant-a")
        .await
        .unwrap()
}

async fn tenant_object_store_shard_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let shard_range = TimeRange::new(0, 30 * SECONDS).unwrap();
    let indexes = api_seconds_block(&dir, TenantB::Present);
    write_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        shard_range,
        &indexes.labels,
        &indexes.blocks,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store_shard(dir, &store, &prefix, "tenant-a", shard_range)
        .await
        .unwrap()
}

async fn tenant_object_store_shard_catalog_fixture() -> QuerierState {
    let fixture = loki_forwarded_tenant_object_store_shard_catalog_service_fixture().await;
    let store = fixture.store;
    let dir = fixture.dir;

    QuerierState::from_tenant_object_store_shards(
        dir,
        &store,
        &ObjectPath::from("indexes"),
        "tenant-a",
        TimeRange::new(0, 19).unwrap(),
    )
    .await
    .unwrap()
}

/// A querier over an index that still names a block whose object a retention
/// sweep deleted.
///
/// Two blocks are written and the manifest records both. The second block's
/// object is then deleted, which is what a sweep does to a live querier: the
/// index is unchanged, and the bytes are gone. Both blocks hold the `api`
/// series, so every query for `api` plans both and meets the gap.
struct RetentionSweptFixture {
    app: Router,
    surviving_bytes: u64,
    swept_bytes: u64,
}

async fn retention_swept_fixture() -> RetentionSweptFixture {
    let mut index = ObjectStoreIndex::new();
    let api = index.series("tenant-a", api_prod_labels());
    let web = index.series(
        "tenant-a",
        log_labels([LabelPair::new("app", "web"), LabelPair::new("env", "prod")]),
    );

    let surviving_bytes = index
        .block(
            &block_key(
                "tenant-a",
                BlockSpan {
                    first: 10,
                    last: 19,
                },
            ),
            vec![LogRow::new(api, 10, r#"{"status":200}"#, BTreeMap::new())],
        )
        .await;

    let swept_key = block_key(
        "tenant-a",
        BlockSpan {
            first: 20,
            last: 29,
        },
    );
    let swept_bytes = index
        .block(
            &swept_key,
            vec![
                LogRow::new(api, 20, r#"{"status":500}"#, BTreeMap::new()),
                LogRow::new(web, 29, r#"{"status":503}"#, BTreeMap::new()),
            ],
        )
        .await;
    index.write_manifest("tenant-a").await;

    // The sweep itself. The manifest still names the block.
    index
        .store
        .delete(&log_block_object_path(&index.prefix, &swept_key))
        .await
        .unwrap();

    let app = index
        .router(
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
        )
        .await;
    RetentionSweptFixture {
        app,
        surviving_bytes,
        swept_bytes,
    }
}

/// A retention sweep deletes block objects while queriers run, so every read
/// surface that plans a block can meet one that is gone. None of them may
/// fail the whole request for it: a valid query answers with the blocks that
/// remain.
///
/// The metadata surfaces degrade differently from the analytics ones, and
/// both shapes are pinned here. `/labels`, `/label/{name}/values` and
/// `/series` fall back to the fingerprints the index still records for the
/// swept block, so `web` survives in the answer even though its rows are
/// gone. `/index/stats`, `/patterns` and `/detected_fields` have no such
/// index-level fallback, so they answer from the surviving block alone.
#[tokio::test]
async fn a_retention_swept_block_degrades_every_read_surface_instead_of_failing() {
    let fixture = retention_swept_fixture().await;
    let app = fixture.app;
    let surviving_bytes = fixture.surviving_bytes;
    let swept_bytes = fixture.swept_bytes;

    let cases = vec![
        UriResponseCase {
            uri: "/loki/api/v1/labels?start=0.000000010&end=0.000000029",
            expected: json!({"status": "success", "data": ["app", "env"]}),
        },
        UriResponseCase {
            uri: "/loki/api/v1/label/app/values?start=0.000000010&end=0.000000029",
            expected: json!({"status": "success", "data": ["api", "web"]}),
        },
        UriResponseCase {
            uri: "/loki/api/v1/series?match%5B%5D=%7Benv%3D%22prod%22%7D&start=0.000000010&end=0.000000029",
            expected: json!({
                "status": "success",
                "data": [
                    {"app": "api", "env": "prod"},
                    {"app": "web", "env": "prod"},
                ],
            }),
        },
        UriResponseCase {
            uri: "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000029",
            expected: json!({
                "streams": 1,
                "chunks": 2,
                "entries": 1,
                "bytes": surviving_bytes + swept_bytes,
            }),
        },
        UriResponseCase {
            uri: "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000029&step=1000000000",
            expected: json!({
                "status": "success",
                "data": [
                    {"pattern": r#"{"status":"<_>"}"#, "samples": [[0, 1]]},
                ],
            }),
        },
        UriResponseCase {
            uri: "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000029&limit=10",
            expected: json!({
                "fields": [

                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"],
                        "jsonPath": ["status"],
                    },
                ],
                "limit": 10,
            }),
        },
    ];

    for case in cases {
        let response = Tenant("tenant-a").get_json(&app, case.uri).await;
        let status = response.status;
        let body = response.body;

        assert!(
            status == StatusCode::OK,
            "{} answers 200 despite the swept block",
            case.uri
        );
        assert!(body == case.expected, "{}", case.uri);
    }
}

/// The tolerance covers an absent block object and nothing else.
///
/// Here the block object is PRESENT and holds bytes that are not a Parquet
/// block, which is a real fault in the stored data rather than a sweep. The
/// request fails, and it must keep failing: a decode error that degraded to a
/// skip would shorten every result with nothing to say so.
///
/// The status pins today's mapping, where `HttpQueryError::BlockStore` lands
/// in the `BAD_REQUEST` arm. The mapping itself is a separate question, since
/// a malformed stored block is a server-side fault and not a client error.
#[tokio::test]
async fn a_present_but_malformed_block_still_fails_the_request() {
    let mut index = ObjectStoreIndex::new();
    let api = index.series("tenant-a", api_prod_labels());

    let key = block_key(
        "tenant-a",
        BlockSpan {
            first: 10,
            last: 19,
        },
    );
    index
        .block(
            &key,
            vec![LogRow::new(api, 10, r#"{"status":200}"#, BTreeMap::new())],
        )
        .await;
    index.write_manifest("tenant-a").await;

    // The object stays, and its bytes stop being a block.
    index
        .store
        .put(
            &log_block_object_path(&index.prefix, &key),
            b"not a parquet block".to_vec().into(),
        )
        .await
        .unwrap();

    let app = index
        .router(
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
        )
        .await;

    let response = Tenant("tenant-a").get(&app, "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0.000000010&end=0.000000019")
    .await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
}
