//! Building blocks shared by every signal's Prometheus service metrics.
//!
//! Each signal (metrics, logs, traces, profiles) exports one
//! `prometheus-client` [`Registry`] under its own prefix, such as
//! `krabka_traces`, wrapped in `Arc<Mutex<…>>` so the `/metrics` exporter can
//! lock it. The metric spec is uniform across the signals, so this module
//! holds the parts that are the same everywhere: the [`SharedRegistry`] type,
//! the label sets, the ingest and query instruments and their bucket layouts,
//! the [`PipelineInstruments`] bundles, the registry constructors, and the
//! `/metrics` exporter. Each signal's own `metrics` module adds only its
//! service-specific instruments.
//!
//! `prometheus-client` appends `_total` to counters at encode time, so counter
//! names are registered WITHOUT the suffix.

use std::sync::Arc;

// The field types of [`PipelineInstruments`], so that a signal's metrics
// module names every shared instrument through this one module.
pub use krabka_blockstore::ObjectStoreMetrics;
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

use crate::RoleKind;
pub use crate::{
    compaction_metrics::CompactionMetrics, wal_consumer_metrics::WalConsumerMetrics,
    wal_produce::WalProduceMetrics,
};

mod ingest_instruments;
mod ingest_push_measurement;
mod metrics_router;
mod pipeline_instruments;
mod query_instruments;
mod registry_scope;
mod request_outcome;
mod route_label;
mod route_status_label;
mod shared_registry;
mod status_label;
mod tenant_label;

pub use self::{
    ingest_instruments::{IngestHelpText, IngestInstruments, IngestRequest},
    ingest_push_measurement::IngestPushMeasurement,
    metrics_router::{encode_registry, metrics_router},
    pipeline_instruments::PipelineInstruments,
    query_instruments::{QueryHelpText, QueryInstruments, QueryRequest},
    registry_scope::{RoleRegistry, register_for_role, register_in_new_registry},
    request_outcome::RequestOutcome,
    route_label::RouteLabel,
    route_status_label::RouteStatusLabel,
    shared_registry::SharedRegistry,
    status_label::StatusLabel,
    tenant_label::TenantLabel,
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use krabka_blockstore::ObjectStoreOperation;
    use krabka_units::{bytes, millis, secs};
    use tower::ServiceExt as _;

    use super::*;

    const INGEST_HELP: IngestHelpText = IngestHelpText {
        requests: "requests",
        bytes: "bytes",
        items: "items",
        duration: "duration",
        wal_append_failures: "wal append failures",
    };

    const QUERY_HELP: QueryHelpText = QueryHelpText {
        requests: "requests",
        duration: "duration",
    };

    fn ingest_request(outcome: RequestOutcome, body_bytes: u32) -> IngestRequest {
        IngestRequest {
            outcome,
            body: bytes(body_bytes),
            items: 1,
            elapsed: millis(10),
        }
    }

    #[test]
    fn ingest_counters_accumulate() {
        let ingest = IngestInstruments::register(&mut Registry::default(), &INGEST_HELP);
        ingest.record(IngestRequest {
            items: 3,
            ..ingest_request(RequestOutcome::Ok, 100)
        });
        ingest.record(IngestRequest {
            items: 2,
            ..ingest_request(RequestOutcome::Ok, 50)
        });
        check!(ingest.bytes.get() == 150);
        check!(ingest.items.get() == 5);
        check!(
            ingest
                .requests
                .get_or_create(&StatusLabel {
                    status: "ok".into()
                })
                .get()
                == 2
        );
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let ingest = IngestInstruments::register(&mut Registry::default(), &INGEST_HELP);
        // A 4xx client error: error outcome, but NOT a WAL failure.
        ingest.record(ingest_request(RequestOutcome::Error, 0));
        assert!(ingest.wal_append_failures.get() == 0);
        // A produce failure: bump explicitly at the WAL error site.
        ingest.record_wal_append_failure();
        assert!(ingest.wal_append_failures.get() == 1);
    }

    #[test]
    fn query_counters_split_by_route_and_status() {
        let query = QueryInstruments::register(&mut Registry::default(), &QUERY_HELP);
        for (route, outcome) in [
            ("query", RequestOutcome::Ok),
            ("query", RequestOutcome::Ok),
            ("query", RequestOutcome::Error),
            ("labels", RequestOutcome::Ok),
        ] {
            query.record(QueryRequest {
                route,
                outcome,
                elapsed: millis(10),
            });
        }
        for (route, status, want) in [
            ("query", "ok", 2u64),
            ("query", "error", 1),
            ("labels", "ok", 1),
        ] {
            assert!(
                query
                    .requests
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
    async fn dimensioned_arguments_export_in_prometheus_base_units() {
        // The instruments hold raw bytes and raw seconds; the quantity seam must
        // scale a `ByteSize`/`Time` into exactly those units, not pass the
        // caller's magnitude through unscaled.
        let ingest = register_in_new_registry("krabka_test", |registry, shared| {
            let ingest = IngestInstruments::register(registry, &INGEST_HELP);
            ingest.record(IngestRequest {
                body: krabka_units::mebibytes(2),
                elapsed: millis(250),
                ..ingest_request(RequestOutcome::Ok, 0)
            });
            shared
        });
        let text = encode_registry(&ingest).await.unwrap();
        for needle in [
            "krabka_test_ingest_bytes_total 2097152",
            "krabka_test_ingest_duration_seconds_sum 0.25",
        ] {
            assert!(text.contains(needle), "missing {needle} in:\n{text}");
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let registry = register_in_new_registry("krabka_test", |registry, shared| {
            IngestInstruments::register(registry, &INGEST_HELP)
                .record(ingest_request(RequestOutcome::Ok, 42));
            shared
        });
        let resp = metrics_router(registry)
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
        assert!(s.contains("krabka_test_ingest_requests_total"), "{s}");
        assert!(s.contains("# EOF"), "{s}");
    }

    /// The shared WAL-consumer, compaction and object-store bundles register
    /// into the registry they are given, so their names come out under that
    /// registry's prefix.
    #[tokio::test]
    async fn pipeline_instruments_carry_the_registry_prefix() {
        let registry = register_in_new_registry("krabka_test", |registry, shared| {
            let pipeline = PipelineInstruments::register(registry);
            pipeline.wal_consumer.record_poll_at(
                &[krabka_client_consumer::ConsumerRecord {
                    topic: "__wal".into(),
                    partition: 2,
                    offset: 17,
                    leader_epoch: 0,
                    timestamp: 1_000,
                    timestamp_type: krabka_client_consumer::TimestampType::CreateTime,
                    key: None,
                    value: None,
                    headers: Vec::new(),
                }],
                3_000,
            );
            pipeline.wal_produce.record_batch_failure(1, 3);
            pipeline.compaction.record_run(true, secs(1));
            pipeline.compaction.record_output(4);
            pipeline
                .object_store
                .record_operation(ObjectStoreOperation::Get, false, millis(20));
            pipeline
                .object_store
                .record_retry(ObjectStoreOperation::Get);
            shared
        });
        let text = encode_registry(&registry).await.unwrap();
        for needle in [
            "krabka_test_wal_consumer_records_total{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_test_wal_consumer_last_consumed_offset{topic=\"__wal\",partition=\"2\"} 17",
            "krabka_test_wal_consumer_polls_total{outcome=\"records\"} 1",
            "krabka_test_wal_consumer_receive_delay_seconds_sum 2.0",
            "krabka_test_wal_partial_batch_appends_total 1",
            "krabka_test_compaction_runs_total{status=\"ok\"} 1",
            "krabka_test_compaction_duration_seconds_count 1",
            "krabka_test_compaction_blocks_total 4",
            "krabka_test_objstore_operations_total{operation=\"get\"} 1",
            "krabka_test_objstore_operation_failures_total{operation=\"get\"} 1",
            "krabka_test_objstore_operation_retries_total{operation=\"get\"} 1",
            "krabka_test_objstore_operation_duration_seconds_count{operation=\"get\"} 1",
        ] {
            assert!(text.contains(needle), "missing {needle} in:\n{text}");
        }
    }

    #[tokio::test]
    async fn role_registry_prefixes_the_role_under_the_signal() {
        let shared: SharedRegistry = Arc::new(Mutex::new(Registry::default()));
        let ingest = register_for_role(
            RoleRegistry {
                shared: Arc::clone(&shared),
                signal_prefix: "krabka_test",
                role: RoleKind::BlockBuilder,
            },
            |registry, _| IngestInstruments::register(registry, &INGEST_HELP),
        )
        .await;
        ingest.record_wal_append_failure();
        let text = encode_registry(&shared).await.unwrap();
        assert!(
            text.contains("krabka_test_block_builder_wal_append_failures_total 1"),
            "{text}"
        );
    }
}
