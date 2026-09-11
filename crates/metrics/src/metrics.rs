//! Prometheus metrics for the metrics-subsystem ingest (distributor) role.
//!
//! This module mirrors the broker's `metrics` pattern. It holds a shared
//! `Registry` wrapped in `Arc<Mutex<…>>`, and a cheaply-`Clone` bundle of
//! metric handles that the ingest handlers clone and increment directly. The
//! registry prefix is `krabka_metrics`. `prometheus-client` appends `_total` to
//! counters at encode time, so this module registers counter names without the
//! suffix.

use std::sync::Arc;

use krabka_blockstore::ObjectStoreMetrics;
use krabka_observability::{
    compaction_metrics::CompactionMetrics, wal_consumer_metrics::WalConsumerMetrics,
};
use krabka_units::prelude::*;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

#[cfg(test)]
mod tests {

    /// `record_blocks_compacted` skips a zero rather than adding it. The
    /// counter would be unchanged either way, so the skip is only observable
    /// as a difference from a non-zero call -- the test therefore records a
    /// real count, then a zero, and checks the total did not move.
    #[test]
    fn compacted_blocks_accumulate_and_a_zero_is_skipped() {
        let metrics = super::ServiceMetrics::new();
        check!(
            metrics.blocks_compacted.get() == 0,
            "a fresh counter is at zero"
        );

        metrics.record_blocks_compacted(3);
        check!(metrics.blocks_compacted.get() == 3);

        // Counts add rather than replace.
        metrics.record_blocks_compacted(4);
        check!(metrics.blocks_compacted.get() == 7, "added, not replaced");

        // A zero leaves the total where it was.
        metrics.record_blocks_compacted(0);
        check!(metrics.blocks_compacted.get() == 7, "zero changed nothing");

        // And a later real count still lands.
        metrics.record_blocks_compacted(1);
        check!(metrics.blocks_compacted.get() == 8);
    }
    use assert2::{assert, check};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_metrics_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, kibibytes(1), 5, millis(12));
        m.record_ingest(false, ByteSize::ZERO, 0, millis(1));
        m.wal_append_failures.inc();
        m.record_ingest_series("tenant-a", 5);
        m.record_blocks_compacted(3);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "krabka_metrics_ingest_requests_total",
            "krabka_metrics_ingest_bytes_total",
            "krabka_metrics_ingest_items_total",
            "krabka_metrics_ingest_duration_seconds",
            "krabka_metrics_wal_append_failures_total",
            "krabka_metrics_ingest_series_total",
            "krabka_metrics_blocks_compacted_total",
            "status=\"ok\"",
            "status=\"error\"",
            "tenant=\"tenant-a\"",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[test]
    fn record_ingest_does_not_touch_wal_failures() {
        let m = ServiceMetrics::new();
        // An error outcome must NOT bump wal_append_failures — that is reserved
        // for actual WAL/produce errors, incremented at the append site.
        m.record_ingest(false, ByteSize::ZERO, 0, Time::ZERO);
        assert!(m.wal_append_failures.get() == 0);
    }

    #[tokio::test]
    async fn dimensioned_arguments_export_in_prometheus_base_units() {
        // The instruments hold raw bytes and raw seconds; the quantity seam must
        // scale a `ByteSize`/`Time` into exactly those units, not pass the
        // caller's magnitude through unscaled.
        let m = ServiceMetrics::new();
        m.record_ingest(true, mebibytes(2), 1, millis(250));

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "krabka_metrics_ingest_bytes_total 2097152",
            "krabka_metrics_ingest_duration_seconds_sum 0.25",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, bytes(42), 1, millis(10));
        let app = metrics_router(m.registry);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("krabka_metrics_ingest_bytes_total"), "{s}");
        assert!(s.contains("# EOF"), "{s}");
    }
    /// The shared WAL-consumer, compaction and object-store bundles register
    /// into this signal's own registry, so their names have to come out under
    /// this signal's prefix. A bundle wired into the wrong registry, or not
    /// registered at all, is invisible from the recording call and shows up
    /// only here.
    #[tokio::test]
    async fn shared_instruments_carry_this_signals_prefix() {
        use krabka_blockstore::ObjectStoreOperation;

        let m = ServiceMetrics::new();
        m.wal_consumer.record_poll_at(
            &[krabka_client_consumer::ConsumerRecord {
                topic: "__wal".into(),
                partition: 2,
                offset: 17,
                leader_epoch: 0,
                timestamp: 1_000,
                key: None,
                value: None,
                headers: Vec::new(),
            }],
            3_000,
        );
        m.compaction.record_run(true, krabka_units::secs(1));
        m.compaction.record_output(4);
        m.object_store
            .record_operation(ObjectStoreOperation::Get, false, krabka_units::millis(20));
        m.object_store.record_retry(ObjectStoreOperation::Get);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "krabka_metrics_wal_consumer_records_total{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_metrics_wal_consumer_last_consumed_offset{topic=\"__wal\",partition=\"2\"} 17",
            "krabka_metrics_wal_consumer_polls_total{outcome=\"records\"} 1",
            "krabka_metrics_wal_consumer_receive_delay_seconds_sum 2.0",
            "krabka_metrics_compaction_runs_total{status=\"ok\"} 1",
            "krabka_metrics_compaction_duration_seconds_count 1",
            "krabka_metrics_compaction_blocks_total 4",
            "krabka_metrics_objstore_operations_total{operation=\"get\"} 1",
            "krabka_metrics_objstore_operation_failures_total{operation=\"get\"} 1",
            "krabka_metrics_objstore_operation_retries_total{operation=\"get\"} 1",
            "krabka_metrics_objstore_operation_duration_seconds_count{operation=\"get\"} 1",
        ] {
            assert2::assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }
}

mod export;
mod metrics_router;
mod service_metrics;
mod shared_registry;
mod status_label;
mod tenant_label;

use export::export;
pub use metrics_router::metrics_router;
pub use service_metrics::ServiceMetrics;
pub use shared_registry::SharedRegistry;
pub use status_label::StatusLabel;
pub use tenant_label::TenantLabel;
