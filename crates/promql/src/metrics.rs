//! Prometheus metrics for the metrics-subsystem query (querier) role.
//!
//! This module mirrors the `metrics` pattern of the broker: a shared `Registry`
//! in an `Arc<Mutex<…>>`, and a bundle of metric handles that is cheap to
//! `Clone`. The query handlers clone the bundle and increment the handles
//! directly. The registry prefix is `krabka_metrics`. `prometheus-client`
//! appends `_total` to counters at encode time, so this module registers counter
//! names without the suffix.
//!
//! This bundle has the same shape as the bundle of the ingest crate
//! (`krabka_metrics::metrics`). Both processes export under the same
//! `krabka_metrics` prefix, but they run in separate binaries.

use std::sync::Arc;

use krabka_units::prelude::*;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

#[cfg(test)]
mod tests {
    use assert2::check;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_metrics_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        // Exercise the ingest helpers too so every counter family materializes
        // a sample line (an empty Family emits only # HELP/# TYPE metadata,
        // which carry the name WITHOUT the `_total` suffix).
        m.record_ingest(true, kibibytes(1), 5, millis(12));
        m.wal_append_failures.inc();
        m.record_query("query", true, millis(50));
        m.record_query("query_range", false, millis(1500));
        m.record_query("series", true, millis(200));
        m.record_query("labels", true, millis(100));
        m.record_query("label_values", true, millis(100));
        // Engine-eval metrics: an instant success, a range failure, and some
        // in-flight tracking so every new metric materializes a sample line.
        m.record_eval("instant", true, millis(20));
        m.record_eval("range", false, millis(1200));
        m.query_started();
        m.query_started();
        m.query_finished();

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "krabka_metrics_ingest_requests_total",
            "krabka_metrics_ingest_bytes_total",
            "krabka_metrics_ingest_items_total",
            "krabka_metrics_ingest_duration_seconds",
            "krabka_metrics_wal_append_failures_total",
            "krabka_metrics_query_requests_total",
            "krabka_metrics_query_duration_seconds",
            "krabka_metrics_query_eval_duration_seconds",
            "krabka_metrics_query_errors_total",
            "krabka_metrics_active_queries",
            "route=\"query\"",
            "route=\"query_range\"",
            "status=\"error\"",
            // The `r#type` field must encode as the bare `type` label key.
            "type=\"instant\"",
            "type=\"range\"",
            // One `query_started` is still outstanding (2 inc, 1 dec) → gauge == 1.
            "krabka_metrics_active_queries 1",
        ] {
            assert2::assert!(buf.contains(needle));
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = ServiceMetrics::new();
        m.record_query("query", true, millis(10));
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
        check!(resp.status() == StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), kibibytes(64).bytes_usize())
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        check!(s.contains("krabka_metrics_query_requests_total"), "{s}");
        check!(s.contains("# EOF"), "{s}");
    }
}

// === split-modules: generated submodules ===
mod export;
mod metrics_router;
mod query_type_label;
mod route_label;
mod route_status_label;
mod service_metrics;
mod shared_registry;
mod status_label;

use export::export;
pub use metrics_router::metrics_router;
pub use query_type_label::QueryTypeLabel;
pub use route_label::RouteLabel;
pub use route_status_label::RouteStatusLabel;
pub use service_metrics::ServiceMetrics;
pub use shared_registry::SharedRegistry;
pub use status_label::StatusLabel;
