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
//! [`krabka_observability::service_metrics`] holds the parts that every signal
//! shares.

pub use krabka_observability::service_metrics::{
    RouteLabel, RouteStatusLabel, SharedRegistry, StatusLabel, metrics_router,
};
use krabka_units::{Time, convert::TimeExt};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

#[cfg(test)]
mod tests {
    use krabka_observability::service_metrics::{
        IngestRequest, QueryRequest, RequestOutcome, encode_registry,
    };
    use krabka_units::prelude::*;

    use super::{RuleEvaluationOutcome, ServiceMetrics};

    #[tokio::test]
    async fn registry_has_metrics_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        // Exercise the ingest helpers too so every counter family materializes
        // a sample line (an empty Family emits only # HELP/# TYPE metadata,
        // which carry the name WITHOUT the `_total` suffix).
        m.record_ingest(IngestRequest {
            outcome: RequestOutcome::Ok,
            body: kibibytes(1),
            items: 5,
            elapsed: millis(12),
        });
        m.ingest.wal_append_failures.inc();
        for (route, outcome, elapsed) in [
            ("query", RequestOutcome::Ok, millis(50)),
            ("query_range", RequestOutcome::Error, millis(1500)),
            ("series", RequestOutcome::Ok, millis(200)),
            ("labels", RequestOutcome::Ok, millis(100)),
            ("label_values", RequestOutcome::Ok, millis(100)),
        ] {
            m.record_query(QueryRequest {
                route,
                outcome,
                elapsed,
            });
        }
        // Engine-eval metrics: an instant success, a range failure, and some
        // in-flight tracking so every new metric materializes a sample line.
        m.record_eval("instant", RequestOutcome::Ok, millis(20));
        m.record_eval("range", RequestOutcome::Error, millis(1200));
        m.query_started();
        m.query_started();
        m.query_finished();
        m.record_ruler_rule(RuleEvaluationOutcome::Failed);
        m.record_ruler_group(0.25);
        m.ruler_owner.set(1);
        m.ruler_producer_id.set(42);
        m.ruler_producer_epoch.set(3);
        m.ruler_lease_renew_failures.inc();
        m.ruler_failover_duration_seconds.set(1.5);
        m.object_store
            .record_retry(krabka_blockstore::ObjectStoreOperation::Get);
        m.wal_consumer.record_partition_assigned("metrics", 0);

        let buf = encode_registry(&m.registry).await.unwrap();
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
            "krabka_metrics_rule_evaluation_failures_total",
            "krabka_metrics_rule_group_last_duration_seconds 0.25",
            "krabka_metrics_ruler_owner 1",
            "krabka_metrics_ruler_producer_id 42",
            "krabka_metrics_ruler_producer_epoch 3",
            "krabka_metrics_ruler_lease_renew_failures_total 1",
            "krabka_metrics_ruler_failover_duration_seconds 1.5",
            "krabka_metrics_objstore_operation_retries_total",
            "krabka_metrics_wal_consumer_partition_owned",
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
}

mod query_type_label;
mod rule_evaluation_outcome;
mod service_metrics;

pub use self::{
    query_type_label::QueryTypeLabel, rule_evaluation_outcome::RuleEvaluationOutcome,
    service_metrics::ServiceMetrics,
};
