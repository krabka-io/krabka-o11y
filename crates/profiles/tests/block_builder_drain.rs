//! The profiles block-builder must flush and commit what it holds when it is
//! asked to stop.
//!
//! The block-builder buffers WAL records and writes a block only once the
//! buffer is full or old enough. A stop that returns from the middle of that
//! window throws the buffer away and leaves the offset behind it uncommitted,
//! so the next start replays the window -- which is what every rolling restart
//! is. This suite boots a real broker, puts a record in the WAL that no
//! ordinary flush rule will reach, cancels the builder, and asserts on the
//! block and the committed offset the drain is supposed to leave behind.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_blockstore::ProfileIndex;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::Producer;
use krabka_profiles::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalSample, WalSymbolSet,
    blockbuilder::{BlockBuilderConfig, run_with_config},
    distributor::{KafkaSink, WalSink as _},
};
use krabka_units::{Time, hours, millis};
use object_store::{ObjectStore, memory::InMemory};
use tokio_util::sync::CancellationToken;

const GROUP_ID: &str = "krabka-profiles-block-builder-drain";

/// Long enough that no ordinary flush can fire during the test: the only way a
/// block reaches the store is the drain.
const UNREACHABLE_FLUSH_RECORDS: usize = 10_000;
const UNREACHABLE_FLUSH_MAX_AGE: Time = hours(24);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_the_block_builder_flushes_and_commits_what_it_buffered() {
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

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = BlockBuilderConfig::new(bootstrap.clone(), Arc::clone(&store));
    let index_key = config.index_key.clone();
    config.group_id = GROUP_ID.to_string();
    config.flush_records = UNREACHABLE_FLUSH_RECORDS;
    config.flush_max_age = UNREACHABLE_FLUSH_MAX_AGE;
    config.poll_timeout = millis(100);

    let shutdown = CancellationToken::new();
    let builder = tokio::spawn(run_with_config(config, shutdown.clone()));

    // Real-time wait, not a progress poll: the builder buffering a record is
    // not observable from outside, so this is a budget generously above the
    // 100ms poll loop it is running. Nothing must have been written yet -- a
    // block here would mean an ordinary flush fired and the drain went
    // untested.
    tokio::time::sleep(Duration::from_secs(5)).await;
    check!(
        indexed_block_count(&store, &index_key).await == 0,
        "no ordinary flush may fire before the drain"
    );

    shutdown.cancel();
    let drained = tokio::time::timeout(Duration::from_secs(30), builder)
        .await
        .expect("the block-builder returns after cancellation")
        .expect("block-builder task");
    assert!(drained.is_ok());

    // The buffered record became a durable block ...
    assert!(indexed_block_count(&store, &index_key).await > 0);
    // ... and the offset behind it was committed, so a restart in the same
    // group replays nothing.
    assert!(replayed_records(&bootstrap).await == 0);
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

/// Every block the index snapshot in the store names.
///
/// A block reaches the store and the index names it in the same flush, so this
/// is the block-builder's own record of what it made durable.
async fn indexed_block_count(store: &Arc<dyn ObjectStore>, index_key: &str) -> usize {
    match ProfileIndex::load_latest_snapshot(store, index_key).await {
        Ok(index) => index.all_blocks().len(),
        // No snapshot at all: nothing was flushed.
        Err(_) => 0,
    }
}

/// What a restart in the block-builder's own consumer group would be handed.
///
/// An uncommitted offset replays the record; a committed one does not.
async fn replayed_records(bootstrap: &str) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(GROUP_ID)
        .client_id("profiles-block-builder-drain-restart")
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
