//! What the block-builder drain suites share: a broker holding one WAL
//! record, a block-builder that has consumed it, and probes of what its drain
//! left behind.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_blockstore::ProfileIndex;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::Producer;
use krabka_profiles::{
    PROFILES_WAL_TOPIC, ProfileRecord, ProfilesError, WalSample, WalSymbolSet,
    blockbuilder::{BlockBuilderConfig, run_with_config},
    distributor::{KafkaSink, WalSink as _},
    metrics::ServiceMetrics,
};
use krabka_units::{Time, hours, millis};
use object_store::ObjectStore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use self::wal_topic::create_wal_topic;

#[path = "../../src/bin/krabka-profiles/wal_topic.rs"]
mod wal_topic;

/// Long enough that no ordinary flush can fire during the test: the only way a
/// block reaches the store is the drain.
const UNREACHABLE_FLUSH_RECORDS: usize = 10_000;
const UNREACHABLE_FLUSH_MAX_AGE: Time = hours(24);

/// A broker with the profiles WAL topic and one record in it.
pub struct OneRecordBroker {
    _broker: BrokerHandle,
    _tempdir: tempfile::TempDir,
    pub bootstrap: String,
}

impl OneRecordBroker {
    pub async fn start() -> Self {
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
        }
    }

    /// A block-builder config for `store` in consumer group `group_id` whose
    /// ordinary flush rules cannot fire, with the metrics it reports to.
    pub fn drain_only_config(
        &self,
        store: &Arc<dyn ObjectStore>,
        group_id: &str,
    ) -> (BlockBuilderConfig, ServiceMetrics) {
        let mut config = BlockBuilderConfig::new(self.bootstrap.clone(), Arc::clone(store));
        config.group_id = group_id.to_string();
        config.flush_records = UNREACHABLE_FLUSH_RECORDS;
        config.flush_max_age = UNREACHABLE_FLUSH_MAX_AGE;
        config.poll_timeout = millis(100);
        let metrics = ServiceMetrics::new();
        config.metrics = Some(metrics.clone());
        (config, metrics)
    }

    /// What a restart in consumer group `group_id` would be handed. An
    /// uncommitted offset replays the record; a committed one does not.
    pub async fn replayed_records(&self, group_id: &str, client_id: &str) -> usize {
        let mut consumer = Consumer::builder()
            .bootstrap(&self.bootstrap)
            .group_id(group_id)
            .client_id(client_id)
            .subscribe([PROFILES_WAL_TOPIC.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .expect("restart consumer");
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut replayed = 0;
            loop {
                let records = consumer
                    .poll(millis(250))
                    .await
                    .expect("poll the restart consumer");
                replayed += records
                    .iter()
                    .filter(|record| record.topic == PROFILES_WAL_TOPIC)
                    .count();
                if consumer.at_log_end().await {
                    return replayed;
                }
            }
        })
        .await
        .expect("the restart consumer reaches the WAL end")
    }
}

/// A running block-builder that has consumed the WAL record.
pub struct ConsumedBuilder {
    shutdown: CancellationToken,
    builder: JoinHandle<Result<(), ProfilesError>>,
}

impl ConsumedBuilder {
    /// Starts a block-builder and waits until it has consumed the WAL record.
    /// Group assignment can take longer than a fixed sleep on a busy CI
    /// runner.
    pub async fn start(config: BlockBuilderConfig, metrics: &ServiceMetrics) -> Self {
        let shutdown = CancellationToken::new();
        let builder = tokio::spawn(run_with_config(config, shutdown.clone()));
        tokio::time::timeout(Duration::from_secs(30), async {
            while metrics.wal_consumer.records(PROFILES_WAL_TOPIC, 0) == 0 {
                assert!(
                    !builder.is_finished(),
                    "block-builder exited before consuming"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the block-builder consumes the WAL record before cancellation");
        Self { shutdown, builder }
    }

    /// Cancels the block-builder and returns what its drain returned.
    pub async fn drain(self) -> Result<(), ProfilesError> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(30), self.builder)
            .await
            .expect("the block-builder returns after cancellation")
            .expect("block-builder task")
    }
}

/// Every block the index snapshot in the store names.
///
/// A block reaches the store and the index names it in the same flush, so this
/// is the block-builder's own record of what it made durable.
pub async fn indexed_block_count(store: &Arc<dyn ObjectStore>, index_key: &str) -> usize {
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
