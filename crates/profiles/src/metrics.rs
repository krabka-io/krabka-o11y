//! Prometheus metrics for the profiles subsystem.
//!
//! This module uses the `OpenMetrics` `prometheus-client` crate. The binary
//! `main` constructs one cheaply-clonable [`ServiceMetrics`] bundle and threads
//! it into the distributor and querier state structs. The ingest and query
//! handler boundaries increment it with the [`ServiceMetrics::record_ingest`]
//! and [`ServiceMetrics::record_query`] helpers. The exporter emits the
//! `OpenMetrics` text format.
//!
//! This module registers counters WITHOUT a `_total` suffix.
//! `prometheus-client` appends the suffix at encode time. The registry prefix
//! is `krabka_profiles`, so the `ingest_requests` counter renders on the wire
//! as `krabka_profiles_ingest_requests_total{status="ok"}`.

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt as _};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

use crate::ids::{IngestBytes, IngestItems};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use krabka_units::millis;
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_profiles_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, IngestBytes(1024), IngestItems(3), millis(12));
        m.record_ingest(false, IngestBytes(0), IngestItems(0), millis(1));
        m.record_wal_append_failure();
        m.record_ingest_samples("tenant-a", 3);
        m.record_blocks_built(2);
        m.record_query("select_series", true, millis(500));
        m.record_query("render", false, millis(100));

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "krabka_profiles_ingest_requests_total",
            "krabka_profiles_ingest_bytes_total",
            "krabka_profiles_ingest_items_total",
            "krabka_profiles_ingest_duration_seconds",
            "krabka_profiles_wal_append_failures_total",
            "krabka_profiles_ingest_samples_total",
            "krabka_profiles_blocks_built_total",
            "krabka_profiles_query_requests_total",
            "krabka_profiles_query_duration_seconds",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        for label in [
            "tenant=\"tenant-a\"",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"select_series\"",
        ] {
            check!(buf.contains(label), "label {label} missing");
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, IngestBytes(42), IngestItems(1), millis(10));
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
        assert!(s.contains("krabka_profiles_ingest_requests_total"), "{s}");
        assert!(s.contains("# EOF"), "{s}");
    }

    #[test]
    fn record_ingest_adds_positive_bytes_and_items() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, IngestBytes(1024), IngestItems(3), millis(12));

        // A positive body/item count must flow through to the cumulative
        // counters. This pins the `> 0` guards: flipping to `< 0` or `== 0`
        // would skip `inc_by` for positive inputs, leaving these at zero.
        check!(m.ingest_bytes.get() == 1024);
        check!(m.ingest_items.get() == 3);
    }

    /// The request counter is split by outcome, so a swapped status label
    /// would report every failure as a success and vice versa. Both are
    /// recorded here and each is checked to have moved only its own series.
    #[test]
    fn ingest_requests_are_counted_under_their_own_outcome() {
        let m = ServiceMetrics::new();
        let count = |status: &str| {
            m.ingest_requests
                .get_or_create(&StatusLabel {
                    status: status.into(),
                })
                .get()
        };

        m.record_ingest(true, IngestBytes(1), IngestItems(1), millis(1));
        check!(count("ok") == 1);
        check!(count("error") == 0);

        m.record_ingest(false, IngestBytes(1), IngestItems(1), millis(1));
        m.record_ingest(false, IngestBytes(1), IngestItems(1), millis(1));
        check!(
            count("ok") == 1,
            "a failure must not land on the success series"
        );
        check!(count("error") == 2);
    }

    /// `record_blocks_built` returns early on zero. The guard has to reject
    /// exactly zero: inverted, it would drop every real count and record only
    /// the empty ones.
    #[test]
    fn blocks_built_counts_everything_except_zero() {
        let m = ServiceMetrics::new();

        m.record_blocks_built(0);
        check!(m.blocks_built.get() == 0, "zero adds nothing");

        m.record_blocks_built(3);
        check!(m.blocks_built.get() == 3);

        m.record_blocks_built(4);
        check!(m.blocks_built.get() == 7, "counts accumulate");
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let m = ServiceMetrics::new();
        // An ok=false request alone must NOT bump wal_append_failures.
        m.record_ingest(false, IngestBytes(0), IngestItems(0), millis(1));
        assert!(m.wal_append_failures.get() == 0);
        // Only the explicit WAL-failure call does.
        m.record_wal_append_failure();
        assert!(m.wal_append_failures.get() == 1);
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
