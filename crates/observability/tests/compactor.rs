// The logs lifecycle has no block merge and no orphan sweep, so this suite
// uses neither step the shared helper names for them.
#[allow(dead_code)]
#[path = "../../blockstore/tests/support/lifecycle_store.rs"]
mod lifecycle_store;
mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures_util::stream::BoxStream;
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogBlockStoreError, LogRow,
    TimeRange, labels, list_tenant_log_index_shard_ranges_from_object_store, log_block_object_path,
    log_tenant_index_manifest_object_path, read_log_block, read_log_block_from_object_store,
    read_log_index_manifest, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shards_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store, write_log_index_manifest,
    write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shards_to_object_store,
};
use krabka_client_consumer::ConsumerError;
use krabka_observability::{
    CompactionError, CompactionFrontier, CompactionOffsetCommitter, CompactorRunError,
    CriticalTaskError, KafkaWalCompactionError, KafkaWalHeader, KafkaWalRecord, LogWalConsumer,
    Offset, OverridesProvider, PartitionIndex, QuerierIndexSource, Role, ServiceConfig,
    ServiceDependencies, ServiceRuntimeError, SharedCompactionFrontier, WalConsumerError,
    WalLogRecord, WalPosition, build_service_router, compact_kafka_wal_records_to_object_store,
    compact_log_block_to_object_store, compact_next_kafka_wal_batch_to_object_store,
    compact_wal_records_to_object_store, read_compaction_frontier_from_object_store,
    run_compactor_once, run_compactor_until_idle, run_compactor_until_shutdown, serve_service,
    serve_service_listener, write_compaction_frontier_to_object_store,
};
use krabka_units::{Time, bytes, hours, millis, minutes};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use support::{LogEntry, kafka_wal_record, log_entry};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
};
use tower::ServiceExt as _;

use self::lifecycle_store::{LifecycleStep, LifecycleStore};

#[derive(Clone)]
struct RecordingObjectStore {
    inner: Arc<dyn ObjectStore>,
    get_paths: Arc<std::sync::Mutex<Vec<String>>>,
    put_paths: Arc<std::sync::Mutex<Vec<(String, usize)>>>,
    /// Every put and every delete in the order the store saw them. The two
    /// path lists above cannot say which came first, and the retention sweep's
    /// contract is exactly an order: the index before the object.
    writes: Arc<std::sync::Mutex<Vec<ObjectStoreWrite>>>,
    put_failures: Arc<PutFailures>,
}

/// Which `put`s a [`RecordingObjectStore`] refuses with a transient error,
/// and how many it has refused.
#[derive(Debug, Default)]
struct PutFailures {
    remaining: std::sync::Mutex<usize>,
    failed: std::sync::atomic::AtomicUsize,
    /// Only a put whose path contains this fails; `None` matches every put.
    matching_path: Option<String>,
}

impl PutFailures {
    /// Whether the put to `location` fails, counting the failure when it does.
    fn take_failure(&self, location: &ObjectPath) -> bool {
        let mut remaining = self.remaining.lock().unwrap();
        let matches_path = self
            .matching_path
            .as_ref()
            .is_none_or(|matching_path| location.as_ref().contains(matching_path));
        if *remaining > 0 && matches_path {
            *remaining -= 1;
            self.failed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        } else {
            false
        }
    }
}

/// One mutating call a [`RecordingObjectStore`] served.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ObjectStoreWrite {
    Put(String),
    Delete(String),
}

impl RecordingObjectStore {
    fn new() -> Self {
        Self::over(
            object_store::memory::InMemory::new(),
            PutFailures::default(),
        )
    }

    fn over(inner: impl ObjectStore, put_failures: PutFailures) -> Self {
        Self {
            inner: Arc::new(inner),
            get_paths: Arc::new(std::sync::Mutex::new(Vec::new())),
            put_paths: Arc::new(std::sync::Mutex::new(Vec::new())),
            writes: Arc::new(std::sync::Mutex::new(Vec::new())),
            put_failures: Arc::new(put_failures),
        }
    }

    /// Fails the first `put` to `inner` once, as a transient error.
    fn fail_first_put(inner: impl ObjectStore) -> Self {
        Self::over(
            inner,
            PutFailures {
                remaining: std::sync::Mutex::new(1),
                ..PutFailures::default()
            },
        )
    }

    /// Fails the first `put` whose path contains `matching_path` once.
    fn fail_first_matching_put(inner: impl ObjectStore, matching_path: &str) -> Self {
        Self::over(
            inner,
            PutFailures {
                remaining: std::sync::Mutex::new(1),
                matching_path: Some(matching_path.to_string()),
                ..PutFailures::default()
            },
        )
    }

    /// Fails every matching `put`, not just the first.
    ///
    /// `fail_first_matching_put` models a transient error that a retry rides
    /// out. This models the write never landing at all, which is how a run
    /// interrupted at that write looks to everything downstream of it.
    fn fail_every_matching_put(inner: impl ObjectStore, matching_path: &str) -> Self {
        Self::over(
            inner,
            PutFailures {
                remaining: std::sync::Mutex::new(usize::MAX),
                matching_path: Some(matching_path.to_string()),
                ..PutFailures::default()
            },
        )
    }

    fn failed_put_count(&self) -> usize {
        self.put_failures
            .failed
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn get_paths(&self) -> Vec<String> {
        self.get_paths.lock().unwrap().clone()
    }

    fn put_paths(&self) -> Vec<(String, usize)> {
        self.put_paths.lock().unwrap().clone()
    }

    fn writes(&self) -> Vec<ObjectStoreWrite> {
        self.writes.lock().unwrap().clone()
    }
}

impl fmt::Debug for RecordingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

impl fmt::Display for RecordingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

async fn read_all_tenant_shard_indexes(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<(LabelIndex, BlockIndex), LogBlockStoreError> {
    read_tenant_log_index_shards_from_object_store(
        store,
        prefix,
        tenant,
        TimeRange::new(i64::MIN, i64::MAX)?,
    )
    .await
}

#[async_trait]
impl ObjectStore for RecordingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.put_failures.take_failure(location) {
            return Err(object_store::Error::Generic {
                store: "failing-put",
                source: "transient put failure".into(),
            });
        }
        self.put_paths
            .lock()
            .unwrap()
            .push((location.to_string(), payload.content_length()));
        self.writes
            .lock()
            .unwrap()
            .push(ObjectStoreWrite::Put(location.to_string()));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_paths.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        use futures_util::StreamExt as _;

        let writes = Arc::clone(&self.writes);
        self.inner
            .delete_stream(locations)
            .inspect(move |result| {
                if let Ok(location) = result {
                    writes
                        .lock()
                        .unwrap()
                        .push(ObjectStoreWrite::Delete(location.to_string()));
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn compactor_writes_block_then_tenant_index_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let mut block_index = BlockIndex::default();
    let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap());

    let descriptor = compact_log_block_to_object_store(
        &store,
        &prefix,
        &key,
        &label_index,
        &mut block_index,
        vec![
            LogRow::new(api, 19, "api error", BTreeMap::new()),
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();

    check!(descriptor.key == key);
    check!(descriptor.fingerprints == BTreeSet::from([api]));
    check!(
        block_index.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor.clone()]
    );

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert!(lines(&rows) == vec!["api ok", "api error"]);

    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();

    assert!(loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()]));
    assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor]
    );
}

/// An empty local object store and indexes for one compaction call to write.
struct CompactionTarget {
    _dir: tempfile::TempDir,
    store: LocalFileSystem,
    prefix: ObjectPath,
    label_index: LabelIndex,
    block_index: BlockIndex,
    committer: RecordingCommitter,
}

impl CompactionTarget {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        Self {
            _dir: dir,
            store,
            prefix: ObjectPath::from("observability/logs"),
            label_index: LabelIndex::default(),
            block_index: BlockIndex::default(),
            committer: RecordingCommitter::default(),
        }
    }

    async fn compact_wal(
        &mut self,
        records: Vec<WalLogRecord>,
    ) -> Result<BlockDescriptor, CompactionError> {
        compact_wal_records_to_object_store(
            &self.store,
            &self.prefix,
            &mut self.label_index,
            &mut self.block_index,
            &mut self.committer,
            records,
        )
        .await
    }

    async fn compact_kafka(
        &mut self,
        records: Vec<KafkaWalRecord>,
    ) -> Result<BlockDescriptor, KafkaWalCompactionError> {
        compact_kafka_wal_records_to_object_store(
            &self.store,
            &self.prefix,
            &mut self.label_index,
            &mut self.block_index,
            &mut self.committer,
            records,
        )
        .await
    }

    async fn compact_next(
        &mut self,
        consumer: &mut RecordingWalConsumer,
    ) -> Result<Option<BlockDescriptor>, CompactorRunError> {
        compact_next_kafka_wal_batch_to_object_store(
            &self.store,
            &self.prefix,
            &mut self.label_index,
            &mut self.block_index,
            consumer,
            millis(1),
        )
        .await
    }

    async fn block_lines(&self, key: &BlockKey) -> Vec<String> {
        read_log_block_from_object_store(&self.store, &self.prefix, key)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.line)
            .collect()
    }

    /// Checks that nothing was indexed for tenant-a.
    fn check_nothing_indexed(&self) {
        check!(self.label_index.label_names("tenant-a").is_empty());
        check!(
            self.block_index
                .match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[])
                .is_empty()
        );
    }
}

fn committed_through(partition: i32, offset: i64) -> Vec<WalPosition> {
    vec![WalPosition {
        partition: PartitionIndex(partition),
        offset: Offset(offset),
    }]
}

fn ok_then_error(partition: i32) -> Vec<KafkaWalRecord> {
    vec![
        kafka_wal_record(
            &wal_record_without_position(10, "api ok"),
            PartitionIndex(partition),
            Offset(42),
        ),
        kafka_wal_record(
            &wal_record_without_position(19, "api error"),
            PartitionIndex(partition),
            Offset(43),
        ),
    ]
}

fn undecodable_kafka_record(partition: i32) -> KafkaWalRecord {
    KafkaWalRecord {
        value: b"not json".to_vec(),
        partition: PartitionIndex(partition),
        offset: Offset(42),
        timestamp_ms: None,
        headers: vec![kafka_header("krabka-format-version", "1")],
    }
}

#[tokio::test]
async fn compactor_commits_partition_offset_after_writing_block_and_index() {
    let mut target = CompactionTarget::new();

    let descriptor = target
        .compact_wal(vec![
            wal_record(10, 42, "api ok"),
            wal_record(19, 43, "api error"),
        ])
        .await
        .unwrap();

    let key = BlockKey::new("tenant-a", 0, 42, 43, TimeRange::new(10, 19).unwrap());
    let api = target
        .label_index
        .insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    check!(descriptor.key == key);
    check!(target.committer.committed == committed_through(0, 43));
    check!(
        target
            .block_index
            .match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor.clone()]
    );

    assert!(target.block_lines(&key).await == ["api ok", "api error"]);

    let (_, loaded_blocks) =
        read_tenant_log_index_manifest_from_object_store(&target.store, &target.prefix, "tenant-a")
            .await
            .unwrap();
    assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor]
    );
}

#[tokio::test]
async fn compactor_decodes_kafka_wal_records_before_writing_block() {
    let mut target = CompactionTarget::new();

    let descriptor = target.compact_kafka(ok_then_error(2)).await.unwrap();

    let key = BlockKey::new("tenant-a", 2, 42, 43, TimeRange::new(10, 19).unwrap());
    let api = target
        .label_index
        .insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    check!(descriptor.key == key);
    check!(target.committer.committed == committed_through(2, 43));
    check!(
        target
            .block_index
            .match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor.clone()]
    );

    assert!(target.block_lines(&key).await == ["api ok", "api error"]);
}

#[tokio::test]
async fn compactor_decodes_native_kafka_log_records_from_headers() {
    let mut target = CompactionTarget::new();

    let descriptor = target
        .compact_kafka(vec![KafkaWalRecord {
            value: b"api error".to_vec(),
            partition: PartitionIndex(2),
            offset: Offset(44),
            timestamp_ms: Some(1),
            headers: vec![
                kafka_header("krabka-wal-record-type", "log-line"),
                kafka_header("krabka-tenant", "tenant-a"),
                kafka_header("krabka-log-timestamp-ns", "1900000"),
                kafka_header("krabka-log-label-app", "api"),
                kafka_header("krabka-log-label-env", "prod"),
                kafka_header("krabka-log-metadata-trace_id", "abc"),
            ],
        }])
        .await
        .unwrap();

    let key = BlockKey::new(
        "tenant-a",
        2,
        44,
        44,
        TimeRange::new(1_900_000, 1_900_000).unwrap(),
    );

    assert!(descriptor.key == key);
    assert!(target.committer.committed == committed_through(2, 44));
    let rows = read_log_block_from_object_store(&target.store, &target.prefix, &key)
        .await
        .unwrap();
    let labels = labels([("app", "api"), ("env", "prod")]);
    let fingerprint = series_fingerprint(&labels);
    assert!(
        rows == vec![LogRow::new(
            fingerprint,
            1_900_000,
            "api error",
            BTreeMap::from([("trace_id".into(), "abc".into())]),
        )]
    );

    let (loaded_labels, loaded_blocks) =
        read_all_tenant_shard_indexes(&target.store, &target.prefix, "tenant-a")
            .await
            .unwrap();
    assert!(loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()]));
    assert!(
        loaded_blocks.match_blocks(
            "tenant-a",
            TimeRange::new(0, 2_000_000).unwrap(),
            &[fingerprint]
        ) == vec![descriptor]
    );
}

#[tokio::test]
async fn compactor_does_not_commit_offset_for_invalid_wal_batch() {
    let mut target = CompactionTarget::new();
    let mut record = wal_record(10, 42, "api ok");
    record.position = None;

    let error = target.compact_wal(vec![record]).await.unwrap_err();

    check!(error.to_string().contains("missing WAL position"));
    check!(target.committer.committed.is_empty());
    target.check_nothing_indexed();
}

#[tokio::test]
async fn compactor_does_not_commit_offset_for_invalid_kafka_wal_payload() {
    let mut target = CompactionTarget::new();

    let error = target
        .compact_kafka(vec![undecodable_kafka_record(2)])
        .await
        .unwrap_err();

    check!(
        error
            .to_string()
            .contains("wal record deserialization failed")
    );
    check!(target.committer.committed.is_empty());
    target.check_nothing_indexed();
}

#[tokio::test]
async fn compactor_polls_kafka_wal_batch_then_commits_after_object_store_write() {
    let mut target = CompactionTarget::new();
    let mut consumer = RecordingWalConsumer::new(vec![ok_then_error(3)]);

    let descriptor = target
        .compact_next(&mut consumer)
        .await
        .unwrap()
        .expect("compacted descriptor");

    let key = BlockKey::new("tenant-a", 3, 42, 43, TimeRange::new(10, 19).unwrap());

    assert!(descriptor.key == key);
    assert!(consumer.committed == committed_through(3, 43));
    assert!(target.block_lines(&key).await == ["api ok", "api error"]);
}

#[tokio::test]
async fn compactor_does_not_commit_polled_batch_when_decode_fails() {
    let mut target = CompactionTarget::new();
    let mut consumer = RecordingWalConsumer::new(vec![vec![undecodable_kafka_record(3)]]);

    let error = target.compact_next(&mut consumer).await.unwrap_err();

    check!(
        error
            .to_string()
            .contains("wal record deserialization failed")
    );
    check!(consumer.committed.is_empty());
    check!(target.label_index.label_names("tenant-a").is_empty());
}

#[tokio::test]
async fn compactor_runtime_compacts_one_polled_batch_from_service_config() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![ok_then_error(4)]));

    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap()
        .expect("compacted descriptor");

    let key = BlockKey::new("tenant-a", 4, 42, 43, TimeRange::new(10, 19).unwrap());

    assert!(descriptor.key == key);
    assert!(object_block_lines(&store, &key).await == ["api ok", "api error"]);
}

async fn object_block_lines(store: &LocalFileSystem, key: &BlockKey) -> Vec<String> {
    read_log_block_from_object_store(store, &ObjectPath::from("observability/logs"), key)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.line)
        .collect()
}

/// A compactor whose data root, and so its delete-request store, is `dir`.
fn deleting_compactor_config(dir: &tempfile::TempDir) -> ServiceConfig {
    let mut config = compactor_config("observability/logs");
    config.data_root = dir.path().to_path_buf();
    config
}

const SECRET_LINES: [&str; 3] = ["api ok", "api secret", "api later secret"];

/// A tenant-a block of three `api` lines: at `first_second`, one second
/// later, and three seconds later. The middle one and the last one contain
/// "secret".
struct SecretBlock {
    /// The fingerprint of the `api` series.
    api: u64,
    /// The WAL offset of the first line; the others follow it.
    first_offset: i64,
    first_second: i64,
}

impl SecretBlock {
    /// The block's key and rows.
    fn key_and_rows(&self) -> (BlockKey, Vec<LogRow>) {
        let SecretBlock {
            api,
            first_offset,
            first_second,
        } = *self;
        let seconds = [first_second, first_second + 1, first_second + 3];
        let key = BlockKey::new(
            "tenant-a",
            0,
            first_offset,
            first_offset + 2,
            TimeRange::new(seconds[0] * 1_000_000_000, seconds[2] * 1_000_000_000).unwrap(),
        );
        let rows = seconds
            .into_iter()
            .zip(SECRET_LINES)
            .map(|(second, line)| LogRow::new(api, second * 1_000_000_000, line, BTreeMap::new()))
            .collect();
        (key, rows)
    }
}

fn api_label_index() -> (LabelIndex, u64) {
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    (label_index, api)
}

/// Accepts a delete request for the secret lines in `range`, then runs one
/// compaction over an empty WAL poll, which writes no new block.
async fn delete_secrets_then_compact_idle(
    config: &ServiceConfig,
    store: &LocalFileSystem,
    range: &str,
) {
    let app = build_service_router(config, ServiceDependencies::default(), Some(store))
        .await
        .unwrap();
    delete_secret_lines(&app, range).await;

    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()]));
    let descriptor = run_compactor_once(config, dependencies, Some(store))
        .await
        .unwrap();
    assert!(descriptor.is_none());
}

fn check_one_block_for(loaded_blocks: &BlockIndex, key: &BlockKey, api: u64) {
    assert!(
        loaded_blocks
            .match_blocks("tenant-a", key.time_range, &[api])
            .len()
            == 1
    );
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_written_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = deleting_compactor_config(&dir);
    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();
    delete_secret_lines(&app, "start=14&end=16").await;
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            [14, 15, 17]
                .into_iter()
                .zip(SECRET_LINES)
                .zip(42..)
                .map(|((second, line), offset)| {
                    kafka_wal_record(
                        &wal_record_without_position(second * 1_000_000_000, line),
                        PartitionIndex(0),
                        Offset(offset),
                    )
                })
                .collect(),
        ]));

    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap()
        .expect("compacted descriptor");

    let key = BlockKey::new(
        "tenant-a",
        0,
        42,
        44,
        TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
    );
    assert!(descriptor.key == key);
    assert!(object_block_lines(&store, &key).await == ["api ok", "api later secret"]);
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_existing_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = deleting_compactor_config(&dir);
    let prefix = ObjectPath::from("observability/logs");
    let (label_index, api) = api_label_index();
    let (key, rows) = SecretBlock {
        api,
        first_offset: 42,
        first_second: 14,
    }
    .key_and_rows();
    let descriptor = write_log_block_to_object_store(&store, &prefix, &key, rows)
        .await
        .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    delete_secrets_then_compact_idle(&config, &store, "start=14&end=16").await;

    assert!(object_block_lines(&store, &key).await == ["api ok", "api later secret"]);
    let (_, loaded_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    check_one_block_for(&loaded_blocks, &key, api);
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_existing_local_manifest_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = deleting_compactor_config(&dir);
    let (label_index, api) = api_label_index();
    let (key, rows) = SecretBlock {
        api,
        first_offset: 42,
        first_second: 14,
    }
    .key_and_rows();
    let descriptor = write_log_block(dir.path(), &key, rows).unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor);
    write_log_index_manifest(dir.path(), &label_index, &block_index).unwrap();

    delete_secrets_then_compact_idle(&config, &store, "start=14&end=16").await;

    let rows = read_log_block(dir.path(), &key).unwrap();
    assert!(lines(&rows) == vec!["api ok", "api later secret"]);
    let (_, loaded_blocks) = read_log_index_manifest(dir.path()).unwrap();
    check_one_block_for(&loaded_blocks, &key, api);
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_existing_shard_blocks() {
    delete_existing_shard(true).await;
}

#[tokio::test]
async fn compactor_runtime_deletes_existing_shard_rows_without_a_catalog() {
    delete_existing_shard(false).await;
}

async fn delete_existing_shard(catalog_exists: bool) {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = deleting_compactor_config(&dir);
    let prefix = ObjectPath::from("observability/logs");
    let (label_index, api) = api_label_index();
    let (key, rows) = SecretBlock {
        api,
        first_offset: 52,
        first_second: 24,
    }
    .key_and_rows();
    let descriptor = write_log_block_to_object_store(&store, &prefix, &key, rows)
        .await
        .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor);
    let shard_range = TimeRange::new(24_000_000_000, 27_000_000_000).unwrap();
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[shard_range],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    if !catalog_exists {
        // Service compaction writes the shard manifest without a catalog.
        store
            .delete(
                &krabka_blockstore::log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a"),
            )
            .await
            .unwrap();
    }

    delete_secrets_then_compact_idle(&config, &store, "start=24&end=26").await;

    assert!(object_block_lines(&store, &key).await == ["api ok", "api later secret"]);
    let (_, loaded_blocks) =
        read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", shard_range)
            .await
            .unwrap();
    check_one_block_for(&loaded_blocks, &key, api);
}

#[tokio::test]
async fn compactor_once_loads_existing_manifest_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let (first_descriptor, second_descriptor) = compact_ok_then_error_runs(&config, &store).await;

    assert_api_blocks_indexed(&store, &[first_descriptor, second_descriptor]).await;
}

#[tokio::test]
async fn compactor_runtime_updates_object_store_shard_catalog_incrementally() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let (first_descriptor, second_descriptor) = compact_ok_then_error_runs(&config, &store).await;

    let prefix = ObjectPath::from("observability/logs");
    let shard_ranges =
        list_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    assert!(
        shard_ranges
            == vec![
                first_descriptor.key.time_range,
                second_descriptor.key.time_range
            ]
    );

    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);
    let (loaded_labels, loaded_blocks) = read_tenant_log_index_shards_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
    )
    .await
    .unwrap();

    assert!(loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()]));
    assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![first_descriptor, second_descriptor]
    );
}

#[tokio::test]
async fn compactor_runtime_rejects_missing_wal_consumer_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");

    let error = run_compactor_once(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("WAL consumer is required"));
}

#[tokio::test]
async fn compactor_runtime_rejects_missing_object_store() {
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::default());

    let error = run_compactor_once(&config, dependencies, None)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("object store is required"));
}

#[tokio::test]
async fn compactor_drain_waits_through_an_empty_poll_with_records_still_pending() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let commits = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dependencies = ServiceDependencies::default().with_wal_consumer(
        RecordingWalConsumer::recording_commits_to(
            vec![
                Vec::new(),
                vec![kafka_wal_record(
                    &wal_record_without_position(30, "api stopping"),
                    PartitionIndex(5),
                    Offset(44),
                )],
            ],
            &commits,
        ),
    );

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    assert!(descriptors.len() == 1);
    assert!(
        commits.lock().unwrap().as_slice()
            == [WalPosition {
                partition: PartitionIndex(5),
                offset: Offset(44),
            }]
    );
}

#[tokio::test]
async fn compactor_runtime_preserves_indexes_across_polled_batches_until_idle() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let descriptors = compact_ok_then_error_batches(&store).await;

    assert!(descriptors.len() == 2);
    assert_api_blocks_indexed(&store, &descriptors).await;
}

#[tokio::test]
async fn compactor_runtime_writes_shard_indexes_without_index_metadata_rewrites() {
    let store = RecordingObjectStore::new();
    let descriptors = compact_ok_then_error_batches(&store).await;

    let prefix = ObjectPath::from("observability/logs");
    let manifest_path =
        krabka_blockstore::log_tenant_index_manifest_object_path(&prefix, "tenant-a").to_string();
    let shard_catalog_path =
        krabka_blockstore::log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a")
            .to_string();
    let manifest_puts = store
        .put_paths()
        .into_iter()
        .filter(|(path, _)| path == &manifest_path)
        .count();
    let shard_catalog_puts = store
        .put_paths()
        .into_iter()
        .filter(|(path, _)| path == &shard_catalog_path)
        .count();
    let (_, loaded_blocks) = read_tenant_log_index_shards_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(i64::MIN, i64::MAX).unwrap(),
    )
    .await
    .unwrap();
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));

    check!(
        manifest_puts == 0,
        "service compactor should not rewrite the full tenant manifest"
    );
    check!(
        shard_catalog_puts == 0,
        "service compactor should not rewrite the shard catalog"
    );
    check!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == descriptors
    );
}

#[tokio::test]
async fn compactor_runtime_writes_shards_with_only_the_new_block() {
    let store = RecordingObjectStore::new();
    let config = compactor_config("observability/logs");
    let dependencies = polls(
        PartitionIndex(5),
        &[
            &[
                polled(Offset(42), log_entry(10, "api first")),
                polled(Offset(43), log_entry(30, "api first later")),
            ],
            &[
                polled(Offset(44), log_entry(20, "api second")),
                polled(Offset(45), log_entry(40, "api second later")),
            ],
            &[],
        ],
    );

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    let prefix = ObjectPath::from("observability/logs");
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let (_, first_blocks) = read_tenant_log_index_shard_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        descriptors[0].key.time_range,
    )
    .await
    .unwrap();
    let (_, second_blocks) = read_tenant_log_index_shard_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        descriptors[1].key.time_range,
    )
    .await
    .unwrap();

    assert!(
        first_blocks.match_blocks("tenant-a", TimeRange::new(0, 50).unwrap(), &[api])
            == vec![descriptors[0].clone()]
    );
    assert!(
        second_blocks.match_blocks("tenant-a", TimeRange::new(0, 50).unwrap(), &[api])
            == vec![descriptors[1].clone()]
    );
}

#[tokio::test]
async fn compactor_runtime_appends_batches_without_loading_tenant_manifest() {
    let store = RecordingObjectStore::new();
    let descriptors = compact_ok_then_error_batches(&store).await;

    let manifest_path = krabka_blockstore::log_tenant_index_manifest_object_path(
        &ObjectPath::from("observability/logs"),
        "tenant-a",
    )
    .to_string();
    let manifest_gets = store
        .get_paths()
        .into_iter()
        .filter(|path| path == &manifest_path)
        .count();

    assert!(descriptors.len() == 2);
    assert!(
        manifest_gets == 0,
        "normal append compaction should not load the historical tenant manifest"
    );
}

#[tokio::test]
async fn compactor_runtime_appends_shard_without_loading_historical_shards() {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let old_range = TimeRange::new(1, 1).unwrap();
    let old_key = BlockKey::new(tenant, 5, 40, 40, old_range);
    let mut old_labels = LabelIndex::default();
    let old_fingerprint = old_labels.insert_series(tenant, labels([("app", "api")]));
    let mut old_blocks = BlockIndex::default();
    old_blocks.insert(krabka_blockstore::BlockDescriptor::new_with_size(
        old_key,
        BTreeSet::from([old_fingerprint]),
        bytes(1),
    ));
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        tenant,
        &[old_range],
        &old_labels,
        &old_blocks,
    )
    .await
    .unwrap();
    store.get_paths.lock().unwrap().clear();

    let config = compactor_config("observability/logs");
    let dependencies = one_api_ok_poll(PartitionIndex(5));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    let old_shard_manifest = krabka_blockstore::index_snapshot_prefix_for_key(
        krabka_blockstore::log_tenant_index_shard_manifest_object_path(&prefix, tenant, old_range)
            .as_ref(),
    );
    assert!(descriptors.len() == 1);
    assert!(
        !store
            .get_paths()
            .into_iter()
            .any(|path| path.starts_with(&old_shard_manifest)),
        "appending a new shard should not load historical shard manifests"
    );
}

#[tokio::test]
async fn compactor_runtime_splits_mixed_tenant_wal_batch_into_tenant_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![
                kafka_wal_record(
                    &wal_record_for_tenant("tenant-a", 10, "tenant a error"),
                    PartitionIndex(5),
                    Offset(42),
                ),
                kafka_wal_record(
                    &wal_record_for_tenant("tenant-b", 11, "tenant b error"),
                    PartitionIndex(5),
                    Offset(43),
                ),
            ],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    assert!(descriptors.len() == 2);
    assert!(
        descriptors
            .iter()
            .map(|descriptor| descriptor.key.tenant.as_str())
            .collect::<Vec<_>>()
            == vec!["tenant-a", "tenant-b"]
    );

    let prefix = ObjectPath::from("observability/logs");
    let (first_tenant_labels, first_tenant_blocks) =
        read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    let (second_tenant_labels, second_tenant_blocks) =
        read_all_tenant_shard_indexes(&store, &prefix, "tenant-b")
            .await
            .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    for (tenant_labels, tenant_blocks, tenant, descriptor) in [
        (
            &first_tenant_labels,
            &first_tenant_blocks,
            "tenant-a",
            &descriptors[0],
        ),
        (
            &second_tenant_labels,
            &second_tenant_blocks,
            "tenant-b",
            &descriptors[1],
        ),
    ] {
        assert!(tenant_labels.label_values(tenant, "app") == BTreeSet::from(["api".into()]));
        assert!(
            tenant_blocks.match_blocks(tenant, TimeRange::new(0, 30).unwrap(), &[api])
                == vec![descriptor.clone()]
        );
    }
}

#[tokio::test]
async fn compactor_runtime_keeps_polling_after_idle_until_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 7, 43, 43, TimeRange::new(19, 19).unwrap());
    let dependencies = polls(
        PartitionIndex(7),
        &[&[], &[polled(Offset(43), log_entry(19, "api error"))], &[]],
    );

    let descriptors = tokio::time::timeout(
        Duration::from_secs(1),
        run_compactor_until_shutdown(&config, dependencies, Some(&store), async {
            let _ = wait_for_log_block(&store, &prefix, &key).await;
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(descriptors.len() == 1);
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert!(lines(&rows) == vec!["api error"]);
}

#[tokio::test]
async fn compactor_runtime_retries_object_store_errors_before_committing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        RecordingObjectStore::fail_first_put(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    compact_one_record_through_one_failed_put(&store).await;
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let rows =
        read_log_block_from_object_store(&store, &ObjectPath::from("observability/logs"), &key)
            .await
            .unwrap();
    assert!(lines(&rows) == vec!["api ok"]);
}

#[tokio::test]
async fn compactor_runtime_retries_shard_manifest_write_errors_before_committing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let store = RecordingObjectStore::fail_first_matching_put(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        "shards/time=10-10/manifest/snapshots/",
    );
    compact_one_record_through_one_failed_put(&store).await;

    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert!(lines(&rows) == vec!["api ok"]);

    let shard_ranges =
        list_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    assert!(shard_ranges == vec![key.time_range]);
}

#[tokio::test]
async fn compactor_runtime_retries_compaction_frontier_write_errors_after_committing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let store = RecordingObjectStore::fail_first_matching_put(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        "compaction-frontier.json",
    );
    compact_one_record_through_one_failed_put(&store).await;

    let persisted_frontier =
        read_compaction_frontier_from_object_store(&store, &ObjectPath::from("observability/logs"))
            .await
            .unwrap();
    assert!(
        persisted_frontier
            == CompactionFrontier::new(i64::MIN)
                .with_partition_offset(PartitionIndex(6), Offset(42))
    );
}

#[tokio::test]
async fn compactor_runtime_advances_shared_compaction_frontier_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let frontier = SharedCompactionFrontier::default();
    let dependencies = polls(
        PartitionIndex(8),
        &[&[polled(Offset(43), log_entry(19, "api error"))], &[]],
    )
    .with_compaction_frontier(frontier.clone());

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    assert!(descriptors.len() == 1);
    assert!(
        frontier.snapshot()
            == krabka_observability::CompactionFrontier::new(i64::MIN)
                .with_partition_offset(PartitionIndex(8), Offset(43))
    );
}

#[tokio::test]
async fn compaction_frontier_round_trips_through_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let frontier = CompactionFrontier::new(0)
        .with_partition_offset(PartitionIndex(8), Offset(43))
        .with_partition_offset(PartitionIndex(9), Offset(55));

    write_compaction_frontier_to_object_store(&store, &prefix, &frontier)
        .await
        .unwrap();
    let loaded = read_compaction_frontier_from_object_store(&store, &prefix)
        .await
        .unwrap();

    assert!(loaded == frontier);
}

#[tokio::test]
async fn compactor_runtime_reloads_shared_frontier_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_frontier = SharedCompactionFrontier::default();
    let first_run = polls(
        PartitionIndex(8),
        &[&[polled(Offset(43), log_entry(19, "api error"))], &[]],
    )
    .with_compaction_frontier(first_frontier);
    run_compactor_until_idle(&config, first_run, Some(&store))
        .await
        .unwrap();

    let restarted_frontier = SharedCompactionFrontier::default();
    let second_run = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()]))
        .with_compaction_frontier(restarted_frontier.clone());
    run_compactor_until_idle(&config, second_run, Some(&store))
        .await
        .unwrap();

    assert!(
        restarted_frontier.snapshot()
            == CompactionFrontier::new(i64::MIN)
                .with_partition_offset(PartitionIndex(8), Offset(43))
    );
}

#[tokio::test]
async fn compactor_runtime_loads_existing_manifest_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_run = one_api_ok_poll(PartitionIndex(6));

    let mut descriptors = run_compactor_until_idle(&config, first_run, Some(&store))
        .await
        .unwrap();

    let second_run = polls(
        PartitionIndex(6),
        &[&[polled(Offset(43), log_entry(19, "api error"))], &[]],
    );
    descriptors.extend(
        run_compactor_until_idle(&config, second_run, Some(&store))
            .await
            .unwrap(),
    );

    assert_api_blocks_indexed(&store, &descriptors).await;
}

#[tokio::test]
async fn compactor_runtime_reprocesses_uncommitted_wal_without_duplicate_manifest_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_run = ServiceDependencies::default().with_wal_consumer(
        RecordingWalConsumer::failing_first_commit(vec![vec![kafka_wal_record(
            &wal_record_without_position(10, "api ok"),
            PartitionIndex(6),
            Offset(42),
        )]]),
    );

    let err = run_compactor_until_idle(&config, first_run, Some(&store))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("coordinator unavailable"));

    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let first_bytes = store
        .get(&log_block_object_path(&prefix, &key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let second_run = one_api_ok_poll(PartitionIndex(6));
    let descriptors = run_compactor_until_idle(&config, second_run, Some(&store))
        .await
        .unwrap();

    let rewritten_bytes = store
        .get(&log_block_object_path(&prefix, &key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert!(lines(&rows) == vec!["api ok"]);
    assert!(rewritten_bytes == first_bytes);

    let (_, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    assert!(descriptors.len() == 1);
    assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == descriptors
    );
}

#[tokio::test]
async fn compactor_service_target_keeps_running_after_idle() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let config = compactor_config("observability/logs");
    let dependencies = one_api_ok_poll(PartitionIndex(6));
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_service(config, dependencies, Some(server_store.as_ref())).await
    });

    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let rows = wait_for_log_block(
        store.as_ref(),
        &ObjectPath::from("observability/logs"),
        &key,
    )
    .await;

    assert!(!server.is_finished());
    server.abort();

    assert!(lines(&rows) == vec!["api ok"]);
}

#[tokio::test]
async fn compactor_service_accumulates_adjacent_small_wal_polls_into_one_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies = polls(
        PartitionIndex(6),
        &[
            &[polled(Offset(42), log_entry(10, "first"))],
            &[polled(Offset(43), log_entry(20, "second"))],
            &[],
        ],
    );

    // The two small polls must land in ONE block spanning both offsets and
    // both timestamps. Waiting for that exact block is a progress poll rather
    // than a run-duration budget: a fixed sleep here asserted that the work
    // had finished by a wall-clock deadline, which under load it sometimes had
    // not -- the suite then failed roughly one run in two hundred, and inside
    // a mutation sweep often enough to refuse four shards of thirty-two.
    let prefix = ObjectPath::from("observability/logs");
    let expected = BlockKey::new("tenant-a", 6, 42, 43, TimeRange::new(10, 20).unwrap());
    let descriptors = run_compactor_until_shutdown(&config, dependencies, Some(&store), async {
        let _ = wait_for_log_block(&store, &prefix, &expected).await;
    })
    .await
    .unwrap();

    assert!(descriptors.len() == 1);
    assert!(descriptors[0].key == expected);

    let rows = read_log_block_from_object_store(&store, &prefix, &descriptors[0].key)
        .await
        .unwrap();
    assert!(lines(&rows) == vec!["first", "second"]);
}

#[tokio::test]
async fn compactor_service_listener_serves_http_while_polling_wal() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let config = compactor_config("observability/logs");
    let dependencies = one_api_ok_poll(PartitionIndex(6));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_service_listener(listener, config, dependencies, Some(server_store.as_ref()))
            .await
            .unwrap();
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET /ready HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.ends_with("ready\n"));

    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let mut rows = None;
    for _ in 0..20 {
        match read_log_block_from_object_store(
            store.as_ref(),
            &ObjectPath::from("observability/logs"),
            &key,
        )
        .await
        {
            Ok(block_rows) => {
                rows = Some(block_rows);
                break;
            }
            // real-time wait (not a progress poll): iteration-count-bounded retry
            // (`for _ in 0..20`); the sleep is the fixed time budget between reads.
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    server.abort();
    let rows = rows.expect("compactor writes block while HTTP server is running");
    assert!(lines(&rows) == vec!["api ok"]);
}

#[tokio::test]
async fn compaction_interrupted_between_block_write_and_index_save_loses_nothing_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let config = compactor_config("observability/logs");
    let block_path = log_block_object_path(&prefix, &key).to_string();

    // The compactor writes the output block first and the tenant's shard
    // manifest second. Failing every write of that manifest cuts the run in
    // exactly the gap between the two: the block is durable, nothing names it.
    let interrupted_store = RecordingObjectStore::fail_every_matching_put(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        "shards/time=10-10/manifest/snapshots/",
    );
    let interrupted_commits = SharedCommitLog::default();
    let interrupted = ServiceDependencies::default().with_wal_consumer(
        RecordingWalConsumer::recording_commits_to(
            vec![vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                PartitionIndex(6),
                Offset(42),
            )]],
            &interrupted_commits,
        ),
    );

    run_compactor_until_idle(&config, interrupted, Some(&interrupted_store))
        .await
        .unwrap_err();

    // The block survived the interruption...
    assert!(interrupted_store.failed_put_count() == 1);
    assert!(list_log_block_paths(&interrupted_store, &prefix).await == vec![block_path.clone()]);
    // ...as an orphan: no shard names it, so no query can reach its rows.
    assert!(
        list_tenant_log_index_shard_ranges_from_object_store(
            &interrupted_store,
            &prefix,
            "tenant-a"
        )
        .await
        .unwrap()
            == Vec::new()
    );
    // The input is safely unconsumed, so the restart below re-reads it.
    assert!(interrupted_commits.lock().unwrap().is_empty());

    // Restart against the same data, with the injected failure gone.
    let restart_store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let restart_commits = SharedCommitLog::default();
    let restarted = ServiceDependencies::default().with_wal_consumer(
        RecordingWalConsumer::recording_commits_to(
            vec![
                vec![kafka_wal_record(
                    &wal_record_without_position(10, "api ok"),
                    PartitionIndex(6),
                    Offset(42),
                )],
                Vec::new(),
            ],
            &restart_commits,
        ),
    );
    let descriptors = run_compactor_until_idle(&config, restarted, Some(&restart_store))
        .await
        .unwrap();

    // The restart finished the work: one block, named by one shard, readable.
    assert!(
        descriptors
            .iter()
            .map(|descriptor| descriptor.key.clone())
            .collect::<Vec<_>>()
            == vec![key.clone()]
    );
    assert!(
        list_tenant_log_index_shard_ranges_from_object_store(&restart_store, &prefix, "tenant-a")
            .await
            .unwrap()
            == vec![key.time_range]
    );
    let rows = read_log_block_from_object_store(&restart_store, &prefix, &key)
        .await
        .unwrap();
    assert!(lines(&rows) == vec!["api ok"]);

    // The interrupted run left no second, unreferenced copy behind: the block
    // key is derived from the tenant, partition, offsets and time range, so the
    // replay rewrites the same object rather than adding one.
    assert!(list_log_block_paths(&restart_store, &prefix).await == vec![block_path]);
    assert!(
        restart_commits.lock().unwrap().as_slice()
            == [WalPosition {
                partition: PartitionIndex(6),
                offset: Offset(42),
            }]
    );
}

#[tokio::test]
async fn compactor_service_stops_serving_when_its_wal_consumer_loop_panics() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let config = compactor_config("observability/logs");
    let trigger = Arc::new(tokio::sync::Notify::new());
    let dependencies = panicking_wal_dependencies(&trigger);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_service_listener(listener, config, dependencies, Some(server_store.as_ref())).await
    });

    // The consumer is parked on the trigger, so the service is demonstrably
    // serving while a live consumer sits behind it.
    assert!(get_ready(addr).await.starts_with("HTTP/1.1 200 OK"));

    // Now kill the consumer loop underneath it.
    trigger.notify_one();
    let joined = server.await.unwrap_err();

    // The panic took the whole role down rather than being reaped behind a
    // still-listening port. A compactor that kept answering here would be
    // reporting itself healthy with nothing draining the WAL.
    assert!(joined.is_panic());
    assert!(TcpStream::connect(addr).await.is_err());
}

/// The querier's WAL hot-tail loop is the one consumer loop this service
/// really does `tokio::spawn`, and a spawned task that panics is reaped
/// silently by default: the `JoinHandle` carries the panic, and dropping it
/// throws the panic away. A querier that did that would keep answering
/// `/ready` with 200 and keep serving whatever the hot tail last held, with
/// nothing reading the WAL behind it.
#[tokio::test]
async fn querier_service_stops_serving_when_its_spawned_wal_consumer_loop_panics() {
    let dir = tempfile::tempdir().unwrap();
    // The querier reads a local manifest at startup; an empty one is enough to
    // get the role serving so the consumer loop is what this test varies.
    write_log_index_manifest(dir.path(), &LabelIndex::default(), &BlockIndex::default()).unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier".to_string(),
        data_root: dir.path().to_path_buf(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        ..ServiceConfig::default()
    };
    let trigger = Arc::new(tokio::sync::Notify::new());
    let dependencies = panicking_wal_dependencies(&trigger);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server =
        tokio::spawn(
            async move { serve_service_listener(listener, config, dependencies, None).await },
        );

    // Parked on the trigger, the hot-tail loop is alive and the querier is ready.
    assert!(get_ready(addr).await.starts_with("HTTP/1.1 200 OK"));

    trigger.notify_one();
    let error = server.await.unwrap().unwrap_err();

    // The role names the dead task and stops, instead of reaping the panic and
    // serving on.
    assert!(matches!(
        error,
        ServiceRuntimeError::CriticalTask(CriticalTaskError("querier WAL hot-tail"))
    ));
    assert!(TcpStream::connect(addr).await.is_err());
}

/// One `GET /ready`, read to end over a `Connection: close` request.
async fn get_ready(addr: std::net::SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET /ready HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[derive(Default)]
struct RecordingCommitter {
    committed: Vec<WalPosition>,
}

impl CompactionOffsetCommitter for RecordingCommitter {
    fn commit_compacted(
        &mut self,
        position: WalPosition,
    ) -> Result<(), krabka_observability::CompactionCommitError> {
        self.committed.push(position);
        Ok(())
    }
}

/// Offsets committed by a consumer the test has handed to `ServiceDependencies`.
///
/// `with_wal_consumer` takes the consumer by value, so `committed` is out of
/// reach once a runtime owns it. Sharing the log keeps "did this run consume
/// the input?" observable after the run.
type SharedCommitLog = Arc<std::sync::Mutex<Vec<WalPosition>>>;

#[derive(Default)]
struct RecordingWalConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
    committed: Vec<WalPosition>,
    failed_commits_remaining: usize,
    commit_log: Option<SharedCommitLog>,
}

impl RecordingWalConsumer {
    fn new(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self {
            batches,
            committed: Vec::new(),
            failed_commits_remaining: 0,
            commit_log: None,
        }
    }

    fn failing_first_commit(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self {
            failed_commits_remaining: 1,
            ..Self::new(batches)
        }
    }

    fn recording_commits_to(
        batches: Vec<Vec<KafkaWalRecord>>,
        commit_log: &SharedCommitLog,
    ) -> Self {
        Self {
            commit_log: Some(Arc::clone(commit_log)),
            ..Self::new(batches)
        }
    }
}

#[async_trait]
impl LogWalConsumer for RecordingWalConsumer {
    async fn is_drained(&mut self) -> bool {
        self.batches.is_empty()
    }

    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError> {
        if self.failed_commits_remaining > 0 {
            self.failed_commits_remaining -= 1;
            return Err(WalConsumerError::Consumer(
                ConsumerError::CoordinatorUnavailable,
            ));
        }
        if let Some(commit_log) = self.commit_log.as_ref() {
            commit_log.lock().unwrap().push(position);
        }
        self.committed.push(position);
        Ok(())
    }
}

/// WAL consumer whose poll loop panics on demand.
///
/// `poll` waits for the test to fire the trigger and then panics inside
/// whichever role's consume loop is driving it. Gating on the trigger rather
/// than on a sleep pins the interleaving exactly: the service is known to have
/// served a request before the consumer behind it dies.
struct PanickingWalConsumer {
    trigger: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl LogWalConsumer for PanickingWalConsumer {
    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        self.trigger.notified().await;
        panic!("injected WAL consumer panic");
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        Ok(())
    }
}

/// Every `.parquet` object under `prefix`, that is every log block the store
/// physically holds, whether or not an index names it.
fn block_keys(index: &BlockIndex) -> Vec<BlockKey> {
    index
        .blocks()
        .iter()
        .map(|block| block.key.clone())
        .collect()
}

async fn list_log_block_paths(store: &dyn ObjectStore, prefix: &ObjectPath) -> Vec<String> {
    use futures_util::StreamExt as _;

    let mut listing = store.list(Some(prefix));
    let mut paths = Vec::new();
    while let Some(meta) = listing.next().await {
        let location = meta.unwrap().location;
        if location.as_ref().ends_with(".parquet") {
            paths.push(location.to_string());
        }
    }
    paths.sort();
    paths
}

fn wal_record(timestamp_ns: i64, offset: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(offset),
        }),
    }
}

async fn wait_for_log_block(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
) -> Vec<LogRow> {
    for _ in 0..50 {
        if let Ok(rows) = read_log_block_from_object_store(store, prefix, key).await {
            return rows;
        }
        // real-time wait (not a progress poll): iteration-count-bounded retry
        // (`for _ in 0..50`); the sleep is the fixed time budget between reads.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    read_log_block_from_object_store(store, prefix, key)
        .await
        .expect("compactor writes expected block")
}

fn wal_record_without_position(timestamp_ns: i64, line: &str) -> WalLogRecord {
    wal_record_for_tenant("tenant-a", timestamp_ns, line)
}

fn wal_record_for_tenant(tenant: &str, timestamp_ns: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: tenant.to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

fn kafka_header(key: &str, value: &str) -> KafkaWalHeader {
    KafkaWalHeader {
        key: key.to_string(),
        value: Some(value.as_bytes().to_vec()),
    }
}

fn compactor_config(index_prefix: &str) -> ServiceConfig {
    ServiceConfig {
        target: Role::BlockBuilder,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-block-builder".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some(index_prefix.to_string()),
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

// ---------------------------------------------------------------------------
// Retention.
// ---------------------------------------------------------------------------

const HOUR_NS: i64 = 3_600 * 1_000_000_000;
const MINUTE_NS: i64 = 60 * 1_000_000_000;

/// A window is measured back from the wall clock the sweep reads, so the block
/// timestamps have to be real epoch nanoseconds rather than the small integers
/// the other tests here use.
fn now_unix_nanos() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the test clock is after the epoch")
            .as_nanos(),
    )
    .expect("the test clock is inside the i64 nanosecond range")
}

/// Writes one block per timestamp, each in a shard of its own, with the tenant
/// manifest and the shard catalog that the compactor writes beside them.
async fn seed_tenant_log_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    timestamps_ns: &[i64],
) -> Vec<BlockKey> {
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series(tenant, labels([("app", "api")]));
    let mut block_index = BlockIndex::default();
    let mut keys = Vec::new();
    for (offset, timestamp_ns) in timestamps_ns.iter().enumerate() {
        let offset = i64::try_from(offset).unwrap();
        let key = BlockKey::new(
            tenant,
            0,
            offset,
            offset,
            TimeRange::new(*timestamp_ns, *timestamp_ns).unwrap(),
        );
        compact_log_block_to_object_store(
            store,
            prefix,
            &key,
            &label_index,
            &mut block_index,
            vec![LogRow::new(
                api,
                *timestamp_ns,
                format!("api ok {offset}"),
                BTreeMap::new(),
            )],
        )
        .await
        .unwrap();
        keys.push(key);
    }
    keys
}

/// One compactor run under a per-tenant retention overrides file.
struct RetentionOverridesRun<'a> {
    config: &'a ServiceConfig,
    store: &'a dyn ObjectStore,
    prefix: &'a ObjectPath,
    overrides_yaml: &'a str,
    /// The block whose object the sweep must delete.
    swept_block: &'a BlockKey,
}

/// Runs the compactor loop over an empty WAL until `run.swept_block` has no
/// object left.
///
/// The sweep is the only thing this loop has to do, so the shutdown future is
/// the condition the sweep produces. A run that never deletes the object stops
/// on the poll budget instead, and the assertions after it then fail on what is
/// still there rather than on a timeout.
async fn run_compactor_with_retention_overrides(run: RetentionOverridesRun<'_>) {
    let RetentionOverridesRun {
        config,
        store,
        prefix,
        overrides_yaml,
        swept_block,
    } = run;
    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(Vec::new()))
        .with_limits(Arc::new(
            OverridesProvider::from_yaml(overrides_yaml).expect("the overrides file parses"),
        ));

    tokio::time::timeout(
        Duration::from_secs(10),
        run_compactor_until_shutdown(config, dependencies, Some(store), async {
            for _ in 0..200 {
                if read_log_block_from_object_store(store, prefix, swept_block)
                    .await
                    .is_err()
                {
                    return;
                }
                // real-time wait (not a progress poll): iteration-count-bounded
                // retry (`for _ in 0..200`); the sleep is the fixed budget
                // between reads.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }),
    )
    .await
    .expect("the compactor loop stops")
    .expect("the compactor loop does not fail");
}

#[tokio::test]
async fn the_retention_sweep_drops_an_expired_block_from_every_index_and_deletes_its_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let (expired, kept) = seed_expired_and_kept_blocks(&store, &prefix).await;

    sweep_with_one_hour_retention(&config, &store, &expired).await;

    // The tenant manifest, the shard manifest and the object, in that order of
    // increasing consequence. A block that left one and stayed in another is
    // exactly the state this sweep exists to avoid.
    let (_, manifest_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    check!(block_keys(&manifest_blocks) == vec![kept.clone()]);
    // Empty manifests fence concurrent appends without deleting their mutable key.
    let expired_shard = read_tenant_log_index_shard_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        expired.time_range,
    )
    .await
    .unwrap();
    check!(expired_shard == (LabelIndex::default(), BlockIndex::default()));
    let (_, shard_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    check!(block_keys(&shard_blocks) == vec![kept.clone()]);
    check!(
        list_log_block_paths(&store, &prefix).await
            == vec![log_block_object_path(&prefix, &kept).to_string()]
    );

    // The block inside the window is still readable, not merely still listed.
    let rows = read_log_block_from_object_store(&store, &prefix, &kept)
        .await
        .unwrap();
    check!(lines(&rows) == vec!["api ok 1"]);
}

#[tokio::test]
async fn the_retention_sweep_keeps_an_empty_shard_manifest_in_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let (expired, kept) = seed_expired_and_kept_blocks(&store, &prefix).await;
    check!(
        read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap()
            == vec![expired.time_range, kept.time_range],
        "the catalog names both shards before the sweep"
    );

    sweep_with_one_hour_retention(&config, &store, &expired).await;

    check!(
        read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap()
            == vec![expired.time_range, kept.time_range]
    );
    let (empty_labels, empty_blocks) = read_tenant_log_index_shard_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        expired.time_range,
    )
    .await
    .unwrap();
    check!((empty_labels, empty_blocks) == (LabelIndex::default(), BlockIndex::default()));

    // A query that reads the tenant after the sweep still answers, and it
    // answers with the block the window keeps.
    let (_, shard_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    check!(block_keys(&shard_blocks) == vec![kept.clone()]);
}

/// A query that read the shard catalog before the sweep rewrote it still
/// answers for the tenant.
///
/// The sweep rewrites the catalog and then deletes the manifest of the shard it
/// emptied, so a reader in between holds a catalog that names a shard whose
/// manifest is gone. The catalog is put back here after the sweep, because that
/// is the state such a reader is in and a read that fetches the catalog itself
/// cannot otherwise be shown it.
#[tokio::test]
async fn a_query_that_read_the_catalog_before_the_sweep_still_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let now_ns = now_unix_nanos();
    let expired = seed_tenant_log_blocks(&store, &prefix, "tenant-a", &[now_ns - 2 * HOUR_NS])
        .await[0]
        .clone();
    let stale_catalog =
        read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    check!(stale_catalog == vec![expired.time_range]);

    sweep_with_one_hour_retention(&config, &store, &expired).await;
    write_tenant_log_index_shard_catalog_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &stale_catalog,
    )
    .await
    .unwrap();

    let (labels_index, shard_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    check!(shard_blocks.blocks().is_empty());
    check!(labels_index.label_values("tenant-a", "app") == BTreeSet::new());
}

#[tokio::test]
async fn each_tenant_is_swept_by_its_own_retention_window() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    // One age, three windows. A block 75 minutes old is outside an hour, inside
    // 90 minutes, and inside "keep forever".
    let written_ns = now_unix_nanos() - 75 * MINUTE_NS;
    let mut keys = BTreeMap::new();
    for tenant in ["tenant-default", "tenant-forever", "tenant-short"] {
        keys.insert(
            tenant,
            seed_tenant_log_blocks(&store, &prefix, tenant, &[written_ns]).await[0].clone(),
        );
    }

    run_compactor_with_retention_overrides(RetentionOverridesRun {
        config: &config,
        store: &store,
        prefix: &prefix,
        overrides_yaml: "defaults:\n  retention_period: \"90m\"\noverrides:\n  tenant-short:\n    \
         retention_period: \"1h\"\n  tenant-forever:\n    retention_period: \"0s\"\n",
        swept_block: &keys["tenant-short"],
    })
    .await;

    let mut expected = vec![
        log_block_object_path(&prefix, &keys["tenant-default"]).to_string(),
        log_block_object_path(&prefix, &keys["tenant-forever"]).to_string(),
    ];
    expected.sort();
    check!(list_log_block_paths(&store, &prefix).await == expected);
}

#[tokio::test]
async fn the_retention_sweep_finds_a_tenant_whose_name_needs_escaping() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    // A separator and a space. Both reach the object key as an escape, so the
    // sweep has to read the tenant back through the same escape the write path
    // used rather than take the path segment as the name.
    let tenant = "team a/b";
    let expired =
        seed_tenant_log_blocks(&store, &prefix, tenant, &[now_unix_nanos() - 2 * HOUR_NS]).await[0]
            .clone();

    run_compactor_with_retention_overrides(RetentionOverridesRun {
        config: &config,
        store: &store,
        prefix: &prefix,
        overrides_yaml: "overrides:\n  \"team a/b\":\n    retention_period: \"1h\"\n",
        swept_block: &expired,
    })
    .await;

    check!(list_log_block_paths(&store, &prefix).await == Vec::<String>::new());
    let (_, manifest_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, tenant)
            .await
            .unwrap();
    check!(manifest_blocks.blocks().is_empty());
}

#[tokio::test]
async fn the_retention_sweep_rewrites_the_index_before_it_deletes_the_object() {
    let store = RecordingObjectStore::new();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let expired = seed_tenant_log_blocks(
        &store,
        &prefix,
        "tenant-a",
        &[now_unix_nanos() - 2 * HOUR_NS],
    )
    .await[0]
        .clone();

    sweep_with_one_hour_retention(&config, &store, &expired).await;

    // The order is the contract. A reader that lists after the manifest put
    // never learns of the block, so it never asks for the object; the reverse
    // order leaves the manifest naming an object that is already gone.
    let writes = store.writes();
    let manifest = ObjectStoreWrite::Put(
        log_tenant_index_manifest_object_path(&prefix, "tenant-a").to_string(),
    );
    let deletion = ObjectStoreWrite::Delete(log_block_object_path(&prefix, &expired).to_string());
    let manifest_rewrite = writes
        .iter()
        .rposition(|write| *write == manifest)
        .expect("the sweep rewrites the tenant manifest");
    let block_delete = writes
        .iter()
        .position(|write| *write == deletion)
        .expect("the sweep deletes the expired block object");
    check!(manifest_rewrite < block_delete);
}

#[tokio::test]
async fn a_block_a_delete_request_empties_has_its_object_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let mut config = compactor_config("observability/logs");
    config.data_root = dir.path().to_path_buf();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let mut block_index = BlockIndex::default();
    let emptied = BlockKey::new(
        "tenant-a",
        0,
        42,
        43,
        TimeRange::new(14_000_000_000, 15_000_000_000).unwrap(),
    );
    let kept = BlockKey::new(
        "tenant-a",
        1,
        52,
        52,
        TimeRange::new(24_000_000_000, 24_000_000_000).unwrap(),
    );
    compact_log_block_to_object_store(
        &store,
        &prefix,
        &emptied,
        &label_index,
        &mut block_index,
        vec![
            LogRow::new(api, 14_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret again", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    compact_log_block_to_object_store(
        &store,
        &prefix,
        &kept,
        &label_index,
        &mut block_index,
        vec![LogRow::new(api, 24_000_000_000, "api ok", BTreeMap::new())],
    )
    .await
    .unwrap();

    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();
    // A window that covers every row of the first block and none of the second.
    delete_secret_lines(&app, "start=14&end=15").await;

    let descriptor = run_compactor_once(
        &config,
        ServiceDependencies::default()
            .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()])),
        Some(&store),
    )
    .await
    .unwrap();
    assert!(descriptor.is_none());

    // The descriptor left the index, so nothing names the object any more. An
    // object no index names is unreachable, and leaving it would grow the
    // bucket for as long as it lives.
    let (_, manifest_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    check!(block_keys(&manifest_blocks) == vec![kept.clone()]);
    check!(
        list_log_block_paths(&store, &prefix).await
            == vec![log_block_object_path(&prefix, &kept).to_string()]
    );
}

#[tokio::test]
async fn a_query_planned_before_the_sweep_still_answers_without_the_deleted_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let (expired, kept) = seed_expired_and_kept_blocks(&store, &prefix).await;

    // A querier that resolves the tenant index per request and caches it. The
    // cache is what makes a query outlive the index it planned against, which
    // is the race the sweep creates: the index this querier holds still names
    // the block after the sweep has deleted the object.
    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", dir.path().display())),
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        querier_dynamic_index_cache_ttl: minutes(10),
        ..ServiceConfig::default()
    };
    let app = build_service_router(&querier_config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    // `end` is exclusive, as `Loki`'s `query_range` defines it, so the window
    // reaches one nanosecond past the newest row.
    let uri = format!(
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start={}&end={}&limit=10",
        expired.time_range.start_ns,
        kept.time_range.end_ns + 1
    );

    let before = app.clone().oneshot(loki_query(&uri)).await.unwrap();
    assert!(before.status() == StatusCode::OK);
    check!(loki_stream_lines(before).await == vec!["api ok 1".to_string(), "api ok 0".to_string()]);

    run_compactor_with_retention_overrides(RetentionOverridesRun {
        config: &compactor_config("observability/logs"),
        store: &store,
        prefix: &prefix,
        overrides_yaml: "overrides:\n  tenant-a:\n    retention_period: \"1h\"\n",
        swept_block: &expired,
    })
    .await;

    // The cached index still names the deleted block. The rest of the answer is
    // still correct, so the query answers without its rows rather than failing.
    let after = app.oneshot(loki_query(&uri)).await.unwrap();
    assert!(after.status() == StatusCode::OK);
    let answer = loki_answer(after).await;
    check!(
        answer["data"]["result"][0]["values"]
            .as_array()
            .map(Vec::len)
            == Some(1)
    );
    check!(answer["data"]["result"][0]["values"][0][1] == "api ok 1");
    // The skipped block is reported rather than passed over in silence. A
    // shorter answer that says nothing reads as a complete one.
    check!(
        answer["warnings"].as_array().map(Vec::len) == Some(1),
        "{answer}"
    );
}

/// One WAL record a poll returns, at `offset`.
#[derive(Clone, Copy)]
struct Polled<'a> {
    offset: Offset,
    entry: LogEntry<'a>,
}

const fn polled(offset: Offset, entry: LogEntry<'_>) -> Polled<'_> {
    Polled { offset, entry }
}

/// Runs the compactor under `observability/logs` in `store` until the WAL is
/// idle, over an `api ok` poll and then an `api error` poll on partition 5.
async fn compact_ok_then_error_batches(store: &dyn ObjectStore) -> Vec<BlockDescriptor> {
    let dependencies = polls(
        PartitionIndex(5),
        &[
            &[polled(Offset(42), log_entry(10, "api ok"))],
            &[polled(Offset(43), log_entry(19, "api error"))],
            &[],
        ],
    );
    run_compactor_until_idle(
        &compactor_config("observability/logs"),
        dependencies,
        Some(store),
    )
    .await
    .unwrap()
}

/// Dependencies whose WAL consumer returns one poll per entry of `batches`,
/// each a list of records on `partition`.
fn panicking_wal_dependencies(trigger: &Arc<tokio::sync::Notify>) -> ServiceDependencies {
    ServiceDependencies::default().with_wal_consumer(PanickingWalConsumer {
        trigger: Arc::clone(trigger),
    })
}

// One poll of the line `api ok` at offset 42, then an empty poll.
fn one_api_ok_poll(partition: PartitionIndex) -> ServiceDependencies {
    polls(
        partition,
        &[&[polled(Offset(42), log_entry(10, "api ok"))], &[]],
    )
}

fn polls(partition: PartitionIndex, batches: &[&[Polled<'_>]]) -> ServiceDependencies {
    ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(
        batches
            .iter()
            .map(|batch| {
                batch
                    .iter()
                    .map(|record| {
                        kafka_wal_record(
                            &wal_record_without_position(
                                record.entry.timestamp_ns,
                                record.entry.line,
                            ),
                            partition,
                            record.offset,
                        )
                    })
                    .collect()
            })
            .collect(),
    ))
}

/// Checks that tenant-a's shard indexes name the `app="api"` label value and
/// hold exactly `expected` for the `api` series.
async fn assert_api_blocks_indexed(store: &dyn ObjectStore, expected: &[BlockDescriptor]) {
    let prefix = ObjectPath::from("observability/logs");
    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(store, &prefix, "tenant-a")
        .await
        .unwrap();
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));

    assert!(loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()]));
    assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api]) == expected
    );
}

/// Runs the compactor once over "api ok" at offset 42, then once more over
/// "api error" at offset 43, and returns the two blocks it wrote.
async fn compact_ok_then_error_runs(
    config: &ServiceConfig,
    store: &LocalFileSystem,
) -> (BlockDescriptor, BlockDescriptor) {
    let first_descriptor = run_compactor_once(
        config,
        polls(
            PartitionIndex(4),
            &[&[polled(Offset(42), log_entry(10, "api ok"))]],
        ),
        Some(store),
    )
    .await
    .unwrap()
    .expect("first compacted descriptor");
    let second_descriptor = run_compactor_once(
        config,
        polls(
            PartitionIndex(4),
            &[&[polled(Offset(43), log_entry(19, "api error"))]],
        ),
        Some(store),
    )
    .await
    .unwrap()
    .expect("second compacted descriptor");
    (first_descriptor, second_descriptor)
}

/// Runs the compactor for 250 ms over one "api ok" record on partition 6, and
/// checks that it wrote one block after `store` refused exactly one put.
async fn compact_one_record_through_one_failed_put(store: &RecordingObjectStore) {
    let config = compactor_config("observability/logs");
    let dependencies = one_api_ok_poll(PartitionIndex(6));

    let descriptors = tokio::time::timeout(
        Duration::from_secs(1),
        run_compactor_until_shutdown(&config, dependencies, Some(store), async {
            // real-time wait (not a progress poll): shutdown future — this sleep is the
            // compactor's run-duration/retry budget, not a poll cadence for a condition.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(descriptors.len() == 1);
    assert!(store.failed_put_count() == 1);
}

/// Seeds tenant-a with a block two hours old and a block one minute old, and
/// returns their keys in that order.
async fn seed_expired_and_kept_blocks(
    store: &LocalFileSystem,
    prefix: &ObjectPath,
) -> (BlockKey, BlockKey) {
    let now_ns = now_unix_nanos();
    let keys = seed_tenant_log_blocks(
        store,
        prefix,
        "tenant-a",
        &[now_ns - 2 * HOUR_NS, now_ns - MINUTE_NS],
    )
    .await;
    (keys[0].clone(), keys[1].clone())
}

/// Runs the compactor with a one-hour retention for tenant-a over the blocks
/// under `observability/logs`, and checks it deletes `expired`.
async fn sweep_with_one_hour_retention(
    config: &ServiceConfig,
    store: &dyn ObjectStore,
    expired: &BlockKey,
) {
    run_compactor_with_retention_overrides(RetentionOverridesRun {
        config,
        store,
        prefix: &ObjectPath::from("observability/logs"),
        overrides_yaml: "overrides:\n  tenant-a:\n    retention_period: \"1h\"\n",
        swept_block: expired,
    })
    .await;
}

fn lines(rows: &[LogRow]) -> Vec<&str> {
    rows.iter().map(|row| row.line.as_str()).collect()
}

/// Requests the deletion of tenant-a's `api` lines that contain "secret" in
/// the window `range` names, and checks the request is accepted.
async fn delete_secret_lines(app: &axum::Router, range: &str) {
    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&{range}"
                ))
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);
}

fn loki_query(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("X-Scope-OrgID", "tenant-a")
        .body(Body::empty())
        .unwrap()
}

async fn loki_answer(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// The log lines of a `Loki` streams response, newest first as the API returns
/// them.
async fn loki_stream_lines(response: axum::response::Response) -> Vec<String> {
    loki_answer(response).await["data"]["result"]
        .as_array()
        .expect("a streams response carries a result array")
        .iter()
        .flat_map(|stream| {
            stream["values"]
                .as_array()
                .expect("a stream carries a values array")
                .iter()
                .map(|value| value[1].as_str().expect("a value is a line").to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn the_retention_period_flag_sweeps_without_an_overrides_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    // No overrides file and no provider handed in: the window comes from the
    // scalar flag, and the compactor loads its own provider to read it. A
    // compactor that read no provider at all would delete nothing and say
    // nothing about it.
    let config = ServiceConfig {
        retention_period: Some(hours(1)),
        ..compactor_config("observability/logs")
    };
    let prefix = ObjectPath::from("observability/logs");
    let (expired, kept) = seed_expired_and_kept_blocks(&store, &prefix).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        run_compactor_until_shutdown(
            &config,
            ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(Vec::new())),
            Some(&store),
            async {
                for _ in 0..200 {
                    if read_log_block_from_object_store(&store, &prefix, &expired)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    // real-time wait (not a progress poll): iteration-count-bounded
                    // retry (`for _ in 0..200`); the sleep is the fixed budget
                    // between reads.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
        ),
    )
    .await
    .expect("the compactor loop stops")
    .expect("the compactor loop does not fail");

    check!(
        list_log_block_paths(&store, &prefix).await
            == vec![log_block_object_path(&prefix, &kept).to_string()]
    );
}

/// A log block goes through every stage of its life on the store the
/// environment names, and a restarted compactor finds what the last one left.
///
/// The logs compactor writes blocks from the WAL and sweeps retention. It does
/// not merge blocks, and it has no orphan sweep, so the test checks neither.
/// By default the store is in memory. See `docs/object_store_contract.md` to
/// run it against a provider.
#[tokio::test]
async fn a_block_survives_its_whole_lifecycle_on_the_configured_store() {
    let mut lifecycle = LifecycleStore::open("logs", "block_lifecycle");
    let store = lifecycle.store();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let now_ns = now_unix_nanos();

    // Flush: one block from the WAL, two hours old.
    let first_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(
                &wal_record_without_position(now_ns - 2 * HOUR_NS, "api old"),
                PartitionIndex(4),
                Offset(42),
            ),
        ]]));
    let old = run_compactor_once(&config, first_run, Some(store.as_ref()))
        .await
        .unwrap()
        .expect("the first block is written");

    // Restart: a new compactor on a new store handle appends to the index the
    // last one saved.
    let store = lifecycle.restart();
    let second_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(
                &wal_record_without_position(now_ns - MINUTE_NS, "api new"),
                PartitionIndex(4),
                Offset(43),
            ),
        ]]));
    let new = run_compactor_once(&config, second_run, Some(store.as_ref()))
        .await
        .unwrap()
        .expect("the second block is written");

    // Query: the index names both blocks, and each block reads.
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let (_, blocks) = read_all_tenant_shard_indexes(store.as_ref(), &prefix, "tenant-a")
        .await
        .unwrap();
    check!(
        blocks.match_blocks("tenant-a", TimeRange::new(0, i64::MAX).unwrap(), &[api])
            == vec![old.clone(), new.clone()]
    );
    for (descriptor, line) in [(&old, "api old"), (&new, "api new")] {
        let rows = read_log_block_from_object_store(store.as_ref(), &prefix, &descriptor.key)
            .await
            .unwrap();
        check!(lines(&rows) == vec![line]);
    }

    // Retention: the two-hour-old block is past a one-hour window.
    run_compactor_with_retention_overrides(RetentionOverridesRun {
        config: &config,
        store: store.as_ref(),
        prefix: &prefix,
        overrides_yaml: "overrides:\n  tenant-a:\n    retention_period: \"1h\"\n",
        swept_block: &old.key,
    })
    .await;
    let (_, blocks) = read_all_tenant_shard_indexes(store.as_ref(), &prefix, "tenant-a")
        .await
        .unwrap();
    check!(blocks.blocks().to_vec() == vec![new.clone()]);
    check!(
        list_log_block_paths(store.as_ref(), &prefix).await
            == vec![log_block_object_path(&prefix, &new.key).to_string()]
    );

    lifecycle
        .finish(&[
            LifecycleStep::Flush,
            LifecycleStep::Restart,
            LifecycleStep::Query,
            LifecycleStep::Retention,
        ])
        .await;
}
