//! Logs-service Prometheus metrics.
//!
//! This metric spec is the same across the LGTM observability services. It is
//! a `prometheus-client` [`Registry`] with prefix `krabka_logs`, wrapped in
//! `Arc<Mutex<…>>` so the `/metrics` exporter can lock it. The cheaply
//! cloneable [`ServiceMetrics`] hands out counter and histogram handles, and
//! the ingest (distributor) and query (querier) handlers increment those
//! handles directly.
//!
//! `prometheus-client` auto-appends `_total` to counters at encode time, so
//! counter names are registered WITHOUT the suffix.

use std::sync::Arc;

use krabka_blockstore::ObjectStoreMetrics;
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

use crate::{
    compaction_metrics::CompactionMetrics, wal_consumer_metrics::WalConsumerMetrics,
    wal_produce::WalProduceMetrics,
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use krabka_units::{bytes, millis};
    use tower::ServiceExt as _;

    use super::{RouteStatusLabel, ServiceMetrics, StatusLabel, metrics_router};

    #[tokio::test]
    async fn registry_has_logs_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, bytes(1_024), 7, millis(10));
        m.record_ingest(false, bytes(0), 0, millis(2));
        m.record_wal_append_failure();
        m.record_ingest_lines("demo", 7);
        m.record_block_written();
        m.record_query("query", true, millis(50));
        m.record_query("query_range", false, millis(200));

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();

        for needle in [
            "krabka_logs_ingest_requests_total",
            "krabka_logs_ingest_bytes_total",
            "krabka_logs_ingest_items_total",
            "krabka_logs_ingest_duration_seconds",
            "krabka_logs_wal_append_failures_total",
            "krabka_logs_ingest_lines_total",
            "krabka_logs_blocks_written_total",
            "krabka_logs_query_requests_total",
            "krabka_logs_query_duration_seconds",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"query\"",
            "tenant=\"demo\"",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[test]
    fn ingest_counters_accumulate() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, bytes(100), 3, millis(10));
        m.record_ingest(true, bytes(50), 2, millis(10));
        check!(m.ingest_bytes.get() == 150);
        check!(m.ingest_items.get() == 5);
        check!(
            m.ingest_requests
                .get_or_create(&StatusLabel {
                    status: "ok".into()
                })
                .get()
                == 2
        );
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let m = ServiceMetrics::new();
        // A 4xx client error: error outcome, but NOT a WAL failure.
        m.record_ingest(false, bytes(0), 0, millis(1));
        assert!(m.wal_append_failures.get() == 0);
        // A produce failure: bump explicitly at the WAL error site.
        m.record_wal_append_failure();
        assert!(m.wal_append_failures.get() == 1);
    }

    #[test]
    fn query_counters_split_by_route_and_status() {
        let m = ServiceMetrics::new();
        m.record_query("query", true, millis(10));
        m.record_query("query", true, millis(20));
        m.record_query("query", false, millis(30));
        m.record_query("labels", true, millis(10));
        for (route, status, want) in [
            ("query", "ok", 2u64),
            ("query", "error", 1),
            ("labels", "ok", 1),
        ] {
            assert!(
                m.query_requests
                    .get_or_create(&RouteStatusLabel {
                        route: route.into(),
                        status: status.into()
                    })
                    .get()
                    == want
            );
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
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/openmetrics-text"), "ct={ct}");
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("krabka_logs_ingest_requests_total"), "{s}");
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
            "krabka_logs_wal_consumer_records_total{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_logs_wal_consumer_last_consumed_offset{topic=\"__wal\",partition=\"2\"} 17",
            "krabka_logs_wal_consumer_polls_total{outcome=\"records\"} 1",
            "krabka_logs_wal_consumer_receive_delay_seconds_sum 2.0",
            "krabka_logs_compaction_runs_total{status=\"ok\"} 1",
            "krabka_logs_compaction_duration_seconds_count 1",
            "krabka_logs_compaction_blocks_total 4",
            "krabka_logs_objstore_operations_total{operation=\"get\"} 1",
            "krabka_logs_objstore_operation_failures_total{operation=\"get\"} 1",
            "krabka_logs_objstore_operation_retries_total{operation=\"get\"} 1",
            "krabka_logs_objstore_operation_duration_seconds_count{operation=\"get\"} 1",
        ] {
            assert2::assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }
}

mod export;
mod metrics_router;
mod route_label;
mod route_status_label;
mod service_metrics;
mod shared_registry;
mod status_label;
mod tenant_label;

use export::export;
pub use metrics_router::metrics_router;
pub use route_label::RouteLabel;
pub use route_status_label::RouteStatusLabel;
pub use service_metrics::ServiceMetrics;
pub use shared_registry::SharedRegistry;
pub use status_label::StatusLabel;
pub use tenant_label::TenantLabel;
