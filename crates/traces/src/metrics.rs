//! Traces-service Prometheus metrics.
//!
//! The metric spec is uniform across the LGTM observability services. It is a
//! `prometheus-client` [`Registry`] with the prefix `krabka_traces`, wrapped in
//! `Arc<Mutex<…>>` so the `/metrics` exporter can lock it. The cheaply
//! cloneable [`ServiceMetrics`] hands out counter and histogram handles, and
//! the ingest and query handlers increment those directly. Ingest is the
//! distributor, and query is the querier.
//!
//! `prometheus-client` auto-appends `_total` to counters at encode time, so
//! counter names are registered WITHOUT the suffix.

use std::sync::Arc;

use krabka_units::prelude::*;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

#[cfg(test)]
mod tests {

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_traces_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, kibibytes(1), 7, millis(10));
        m.record_ingest(false, ByteSize::ZERO, 0, millis(2));
        m.record_wal_append_failure();
        m.record_ingest_spans("tenant-a", 7);
        m.record_block_flushed();
        m.record_query("search", true, 0.05);
        m.record_query("trace_by_id", false, 0.2);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();

        for needle in [
            "krabka_traces_ingest_requests_total",
            "krabka_traces_ingest_bytes_total",
            "krabka_traces_ingest_items_total",
            "krabka_traces_ingest_duration_seconds",
            "krabka_traces_wal_append_failures_total",
            "krabka_traces_ingest_spans_total",
            "krabka_traces_blocks_flushed_total",
            "krabka_traces_query_requests_total",
            "krabka_traces_query_duration_seconds",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"search\"",
            "tenant=\"tenant-a\"",
        ] {
            assert2::assert!(buf.contains(needle));
        }
    }

    #[test]
    fn ingest_counters_accumulate() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, bytes(100), 3, millis(10));
        m.record_ingest(true, bytes(50), 2, millis(10));
        assert2::assert!(m.ingest_bytes.get() == 150);
        assert2::assert!(m.ingest_items.get() == 5);
        assert2::assert!(
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
        m.record_ingest(false, ByteSize::ZERO, 0, Time::ZERO);
        assert2::assert!(m.wal_append_failures.get() == 0);
        // A produce failure: bump explicitly at the WAL error site.
        m.record_wal_append_failure();
        assert2::assert!(m.wal_append_failures.get() == 1);
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
            "krabka_traces_ingest_bytes_total 2097152",
            "krabka_traces_ingest_duration_seconds_sum 0.25",
        ] {
            assert2::assert!(buf.contains(needle));
        }
    }

    #[test]
    fn ingest_spans_split_by_tenant_and_blocks_flushed_accumulate() {
        let m = ServiceMetrics::new();
        m.record_ingest_spans("tenant-a", 3);
        m.record_ingest_spans("tenant-a", 2);
        m.record_ingest_spans("tenant-b", 4);
        // A zero-span request must not create a tenant series.
        m.record_ingest_spans("tenant-c", 0);
        m.record_block_flushed();
        m.record_block_flushed();

        assert2::assert!(
            m.ingest_spans
                .get_or_create(&TenantLabel {
                    tenant: "tenant-a".into()
                })
                .get()
                == 5
        );
        assert2::assert!(
            m.ingest_spans
                .get_or_create(&TenantLabel {
                    tenant: "tenant-b".into()
                })
                .get()
                == 4
        );
        assert2::assert!(m.blocks_flushed.get() == 2);
    }

    #[test]
    fn query_counters_split_by_route_and_status() {
        let m = ServiceMetrics::new();
        m.record_query("search", true, 0.01);
        m.record_query("search", true, 0.02);
        m.record_query("search", false, 0.03);
        m.record_query("tags", true, 0.01);
        for (route, status, want) in [
            ("search", "ok", 2),
            ("search", "error", 1),
            ("tags", "ok", 1),
        ] {
            assert2::assert!(
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
        assert2::assert!(resp.status() == StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert2::assert!(ct.starts_with("application/openmetrics-text"));
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert2::assert!(s.contains("krabka_traces_ingest_requests_total"));
        assert2::assert!(s.contains("# EOF"));
    }
}

// === split-modules: generated submodules ===
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
