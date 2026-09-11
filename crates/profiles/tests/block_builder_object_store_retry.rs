//! The profiles block-builder must ride out an object store that fails and
//! recovers, and must stop at once on one that never will.
//!
//! A flush writes the block, then the profile index snapshot, then commits the
//! WAL offset. Before this, any object-store error at any of those steps left
//! `run_with_config`, left `main`, and ended the role. Nothing was lost --
//! offsets sit behind the write -- but the role came back cold and re-read the
//! same window, so a fault outlasting a restart became an unbounded retry at
//! process granularity. This suite boots a real broker and asserts the two
//! halves of the replacement: a transient failure is ridden out and the offset
//! is committed exactly once, and a permanent one is reported on the first
//! attempt with the offset left where it was.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use assert2::{assert, check};
use async_trait::async_trait;
use futures::stream::BoxStream;
use krabka_blockstore::{ObjectStoreRetryPolicy, ProfileIndex};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::Producer;
use krabka_profiles::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalSample, WalSymbolSet,
    blockbuilder::{BlockBuilderConfig, run_with_config},
    distributor::{KafkaSink, WalSink as _},
};
use krabka_units::{Time, hours, millis};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio_util::sync::CancellationToken;

/// Long enough that no ordinary flush can fire during the test: the only way a
/// block reaches the store is the drain.
const UNREACHABLE_FLUSH_RECORDS: usize = 10_000;
const UNREACHABLE_FLUSH_MAX_AGE: Time = hours(24);

/// A 5xx, a timeout or a reset connection -- what the `object_store` clients
/// report once their own retry budget is spent.
fn transient_failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "FlakyIndexStore",
        source: "injected 503".into(),
    }
}

/// A credential the store refuses. Waiting does not change the answer.
fn permanent_failure() -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: "index".to_string(),
        source: "injected 403".into(),
    }
}

/// An in-memory store whose first `failures` writes *under the index prefix*
/// fail with `error`.
///
/// Only the index writes are made to fail, because the index store is the one
/// the block-builder wraps with [`BlockBuilderConfig::object_store_retry`].
/// The block write is retried by the `BlockWriter` under its own policy and
/// has its own coverage in `krabka-blockstore`; leaving it alone here keeps
/// this test's schedule entirely injected, so nothing sleeps.
#[derive(Debug)]
struct FlakyIndexStore {
    inner: InMemory,
    remaining_failures: AtomicUsize,
    index_put_attempts: AtomicUsize,
    error: fn() -> object_store::Error,
}

impl FlakyIndexStore {
    fn new(failures: usize, error: fn() -> object_store::Error) -> Self {
        Self {
            inner: InMemory::new(),
            remaining_failures: AtomicUsize::new(failures),
            index_put_attempts: AtomicUsize::new(0),
            error,
        }
    }

    fn index_put_attempts(&self) -> usize {
        self.index_put_attempts.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for FlakyIndexStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FlakyIndexStore")
    }
}

#[async_trait]
impl ObjectStore for FlakyIndexStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with("index/") {
            self.index_put_attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    (left > 0).then(|| left - 1)
                })
                .is_ok()
            {
                return Err((self.error)());
            }
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
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

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_rides_out_a_transient_object_store_and_commits_once() {
    let broker = TestBroker::start("krabka-profiles-retry-transient").await;
    let flaky = Arc::new(FlakyIndexStore::new(2, transient_failure));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;

    let (drained, index_key) = broker.drain_with(&store, &flaky).await;

    assert!(drained.is_ok());
    // The snapshot really was retried, not merely written once.
    check!(flaky.index_put_attempts() > 2);
    // The buffered record became a durable block ...
    assert!(indexed_block_count(&store, &index_key).await > 0);
    // ... and the offset behind it was committed exactly once, so a restart in
    // the same group replays nothing.
    check!(broker.replayed_records().await == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_refused_by_the_store_fails_at_once_and_leaves_the_offset() {
    let broker = TestBroker::start("krabka-profiles-retry-permanent").await;
    let flaky = Arc::new(FlakyIndexStore::new(usize::MAX, permanent_failure));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;

    let (drained, _index_key) = broker.drain_with(&store, &flaky).await;

    // Reported rather than retried: a 403 will read the same in four seconds.
    assert!(drained.is_err());
    check!(flaky.index_put_attempts() == 1);
    // And the offset stays behind the data the flush failed to make durable,
    // so a restart re-reads the record instead of losing it.
    check!(broker.replayed_records().await == 1);
}

/// A broker with the profiles WAL topic and one buffered record in it.
struct TestBroker {
    _broker: BrokerHandle,
    _tempdir: tempfile::TempDir,
    bootstrap: String,
    group_id: String,
}

impl TestBroker {
    async fn start(group_id: &str) -> Self {
        let tempdir = tempfile::TempDir::new().expect("tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        create_wal_topic(&bootstrap).await;
        let producer = Producer::builder()
            .bootstrap(&bootstrap)
            .build()
            .await
            .expect("producer build");
        KafkaSink::new(Arc::new(producer))
            .append(profile_record())
            .await
            .expect("append the WAL record");
        Self {
            _broker: broker,
            _tempdir: tempdir,
            bootstrap,
            group_id: group_id.to_string(),
        }
    }

    /// Runs a block-builder against `store` until the record is buffered, then
    /// cancels it so the drain flush happens, and returns what the drain
    /// returned along with the index key it used.
    async fn drain_with(
        &self,
        store: &Arc<dyn ObjectStore>,
        flaky: &Arc<FlakyIndexStore>,
    ) -> (Result<(), krabka_profiles::ProfilesError>, String) {
        let mut config = BlockBuilderConfig::new(self.bootstrap.clone(), Arc::clone(store));
        let index_key = config.index_key.clone();
        config.group_id = self.group_id.clone();
        config.flush_records = UNREACHABLE_FLUSH_RECORDS;
        config.flush_max_age = UNREACHABLE_FLUSH_MAX_AGE;
        config.poll_timeout = millis(100);
        // The injected schedule: the same number of attempts the default
        // allows, with none of its waiting. Raising the default cannot make
        // this suite slower.
        config.object_store_retry = ObjectStoreRetryPolicy::immediate(4);

        let shutdown = CancellationToken::new();
        let builder = tokio::spawn(run_with_config(config, shutdown.clone()));

        // Real-time wait, not a progress poll: the builder buffering a record
        // is not observable from outside, so this is a budget generously above
        // the 100ms poll loop it is running. No ordinary flush can fire in it,
        // because the record and age thresholds above are unreachable, so the
        // store must still be untouched when the wait ends.
        tokio::time::sleep(Duration::from_secs(5)).await;
        check!(
            flaky.index_put_attempts() == 0,
            "no ordinary flush may fire before the drain"
        );

        shutdown.cancel();
        let drained = tokio::time::timeout(Duration::from_secs(30), builder)
            .await
            .expect("the block-builder returns after cancellation")
            .expect("block-builder task");
        (drained, index_key)
    }

    /// What a restart in the block-builder's own consumer group would be
    /// handed. An uncommitted offset replays the record; a committed one does
    /// not.
    async fn replayed_records(&self) -> usize {
        let mut consumer = Consumer::builder()
            .bootstrap(&self.bootstrap)
            .group_id(self.group_id.clone())
            .client_id("profiles-block-builder-retry-restart")
            .subscribe([PROFILES_WAL_TOPIC.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .expect("restart consumer");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut replayed = 0;
        while Instant::now() < deadline {
            let records = consumer
                .poll(millis(250))
                .await
                .expect("poll the restart consumer");
            replayed += records
                .iter()
                .filter(|record| record.topic == PROFILES_WAL_TOPIC)
                .count();
        }
        replayed
    }
}

/// Every block the index snapshot in the store names.
async fn indexed_block_count(store: &Arc<dyn ObjectStore>, index_key: &str) -> usize {
    match ProfileIndex::load_latest_snapshot(store, index_key).await {
        Ok(index) => index.all_blocks().len(),
        // No snapshot at all: nothing was flushed.
        Err(_) => 0,
    }
}

fn profile_record() -> ProfileRecord {
    ProfileRecord {
        tenant: "tenant-a".into(),
        labels: vec![
            ("__name__".into(), "process_cpu".into()),
            ("service_name".into(), "checkout".into()),
            (
                "__profile_type__".into(),
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            ),
        ],
        profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
        samples: vec![WalSample {
            stacktrace_location_refs: vec![0, 1],
            value: 100,
            timestamp_ns: 1_700_000_000_000_000_000,
            span_id: None,
            trace_id: None,
        }],
        symbols: WalSymbolSet {
            strings: vec![String::new(), "main.work".into(), "main.hotloop".into()],
            functions: vec![],
            locations: vec![],
            mappings: vec![],
        },
    }
}

async fn create_wal_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: PROFILES_WAL_TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            krabka_units::secs(5),
        )
        .await
        .expect("create the profiles WAL topic");
}
