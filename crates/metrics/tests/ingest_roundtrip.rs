//! End-to-end metrics ingest, from `remote_write` v1 to the distributor, then
//! to the broker WAL, then to the compactor, then to an object-store block.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{MeteredObjectStore, read_block};
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::Producer;
use krabka_ids::{Offset, PartitionIndex};
use krabka_metrics::{
    CompactionPartitionOffset, MetricBlockKind, MetricsCompactorConfig, SamplePayload, WAL_TOPIC,
    WalRecord, compaction_partition_object_key,
    distributor::{DistributorState, KafkaSink, router},
    metrics::ServiceMetrics,
    run_compactor_consumer_loop,
    wire::pb,
};
use krabka_observability::topic_contract::{
    METRICS_TOPICS, PartitionCount, TopicSettings, provision_topics,
};
use krabka_units::prelude::*;
use object_store::{ObjectStore, memory::InMemory};
use prost::Message;
use tower::ServiceExt as _;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_write_v1_lands_as_block() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    create_metrics_wal_topic(&bootstrap).await;

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .expect("producer build");
    let state = Arc::new(DistributorState::new(Arc::new(KafkaSink::new(Arc::new(
        producer,
    )))));
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/push")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::from(remote_write_v1_body()))
                .expect("request"),
        )
        .await
        .expect("push response");

    assert!(response.status() == StatusCode::NO_CONTENT);

    let wal_record = inspect_wal_record(&bootstrap).await;
    let fingerprint = wal_record.series_fingerprint();
    check!(wal_record.tenant == "tenant-a");
    check!(
        wal_record
            .labels
            .iter()
            .any(|(name, value)| name == "__name__" && value == "up")
    );
    assert!(matches!(
        wal_record.payload,
        SamplePayload::Float {
            timestamp_ms: 100,
            value,
            start_timestamp_ms: None,
        } if (value - 1.0).abs() < f64::EPSILON
    ));

    // The store is wrapped the way `build_object_store` wraps it in the
    // binary, so this round trip exercises the decorator a deployment has
    // rather than a bare store the test made up.
    let metrics = ServiceMetrics::new();
    let object_store: Arc<dyn ObjectStore> =
        MeteredObjectStore::wrap(Arc::new(InMemory::new()), metrics.object_store.clone());
    let mut config = MetricsCompactorConfig::new(bootstrap);
    config.group_id = "metrics-roundtrip-compactor".into();
    config.client_id = "metrics-roundtrip-compactor".into();
    config.poll_timeout = millis(100);
    let runtime = config
        .build_runtime(object_store.clone(), metrics.object_store.clone())
        .expect("compactor runtime");
    let mut consumer = config
        .build_consumer(&metrics.wal_consumer)
        .await
        .expect("compactor consumer");
    let result = run_compactor_consumer_loop(
        &mut consumer,
        &runtime.block_writer,
        &runtime.index_sink,
        runtime.loop_config,
        |poll| poll.compacted_records > 0,
        &metrics,
    )
    .await
    .expect("run compactor");

    check!(result.compacted_records == 1);
    check!(result.writes == 1);
    check!(
        result.committed_offsets
            == vec![CompactionPartitionOffset {
                partition: PartitionIndex(0),
                offset: Offset(1),
            }]
    );

    let block_key = compaction_partition_object_key(
        "tenant-a",
        MetricBlockKind::Float,
        PartitionIndex(0),
        0,
        0,
    );
    let batches = read_block(object_store, &block_key)
        .await
        .expect("read compacted block");
    let rows: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert!(rows == 1);

    let expected_manifest_key = block_key.replace(".parquet", ".index");
    let manifest = runtime
        .index_sink
        .read_manifest(&expected_manifest_key)
        .await
        .expect("read compaction manifest");
    check!(manifest.tenant == "tenant-a");
    check!(manifest.block_key == block_key);
    check!(manifest.row_count == 1);
    check!(fingerprint != 0);

    check_instruments_moved(&metrics).await;
}

/// The instruments are read back through the registry the `/metrics` exporter
/// serves, after a real broker and a real object store have been driven end to
/// end. A handle that was never registered, or registered under a name that
/// does not match, scrapes exactly like a handle that was never incremented,
/// and only a scrape tells them apart.
async fn check_instruments_moved(metrics: &ServiceMetrics) {
    let mut buffer = String::new();
    let registry = metrics.registry.lock().await;
    prometheus_client::encoding::text::encode(&mut buffer, &registry).expect("encode the registry");

    for needle in [
        // The compactor consumed the WAL record the distributor produced, and
        // the offset it reached is the one the broker handed out.
        "krabka_metrics_wal_consumer_records_total{topic=\"__krabka_metrics_wal\",partition=\"0\"} 1",
        "krabka_metrics_wal_consumer_last_consumed_offset{topic=\"__krabka_metrics_wal\",partition=\"0\"} 0",
        "krabka_metrics_wal_consumer_polls_total{outcome=\"records\"}",
        "krabka_metrics_wal_consumer_receive_delay_seconds_count 1",
        // The compactor is the only member of its group, so it owns the
        // partition it read.
        "krabka_metrics_wal_consumer_partition_owned{topic=\"__krabka_metrics_wal\",partition=\"0\"} 1",
        // One flush ran, it succeeded, and it wrote one block.
        "krabka_metrics_compaction_runs_total{status=\"ok\"} 1",
        "krabka_metrics_compaction_duration_seconds_count 1",
        "krabka_metrics_compaction_blocks_total 1",
        "krabka_metrics_blocks_compacted_total 1",
        // The block and its index sidecar reached the object store through the
        // decorator, so the requests are counted and their bytes are counted.
        "krabka_metrics_objstore_operations_total{operation=\"put\"}",
        "krabka_metrics_objstore_operation_duration_seconds_count{operation=\"put\"}",
        "krabka_metrics_objstore_operation_transferred_bytes_total{operation=\"put\"}",
    ] {
        assert!(buffer.contains(needle), "missing {needle} in:\n{buffer}");
    }

    // A run that wrote its block moved no failure series. `prometheus-client`
    // emits no line for a label set nothing touched, so the absence of the
    // error series is the assertion.
    for absent in [
        "krabka_metrics_compaction_runs_total{status=\"error\"}",
        "krabka_metrics_wal_consumer_polls_total{outcome=\"error\"}",
        // A group with one member never rebalances, so no partition moved.
        "krabka_metrics_wal_consumer_partition_revocations_total",
    ] {
        check!(
            !buffer.contains(absent),
            "a clean round trip must not move {absent}:\n{buffer}"
        );
    }
}

/// Provisions the metrics WAL through the topic contract, so this round trip
/// runs against a topic created the way a deployment creates it rather than
/// one the test hand-rolls.
async fn create_metrics_wal_topic(bootstrap: &str) {
    let report = provision_topics(bootstrap, &METRICS_TOPICS, &TopicSettings::single_broker())
        .await
        .expect("provision the metrics topics");
    check!(report.partitions(WAL_TOPIC) == PartitionCount::new(1).ok());
}

async fn inspect_wal_record(bootstrap: &str) -> WalRecord {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("metrics-roundtrip-inspect")
        .client_id("metrics-roundtrip-inspect")
        .subscribe([WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("inspect consumer");
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let records = consumer
            .poll(krabka_units::millis(250))
            .await
            .expect("poll inspect consumer");
        if let Some(record) = records.into_iter().find(|record| record.topic == WAL_TOPIC) {
            assert!(record.partition == 0);
            assert!(record.offset == 0);
            let value = record.value.expect("wal record value");
            return WalRecord::decode(&value).expect("decode wal record");
        }
    }
    panic!("timed out waiting for metrics WAL record");
}

fn remote_write_v1_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: vec![pb::v1::TimeSeries {
            labels: vec![pb::v1::Label {
                name: "__name__".into(),
                value: "up".into(),
            }],
            samples: vec![pb::v1::Sample {
                value: 1.0,
                timestamp: 100,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress")
}
