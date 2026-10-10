//! Traces-service Prometheus metrics.
//!
//! The metric spec is uniform across the LGTM observability services, and
//! [`krabka_observability::service_metrics`] holds the parts they share. It is
//! a `prometheus-client` registry with the prefix `krabka_traces`, wrapped in
//! `Arc<Mutex<…>>` so the `/metrics` exporter can lock it. The cheaply
//! cloneable [`ServiceMetrics`] hands out counter and histogram handles, and
//! the ingest and query handlers increment those directly. Ingest is the
//! distributor, and query is the querier.
//!
//! `prometheus-client` auto-appends `_total` to counters at encode time, so
//! counter names are registered WITHOUT the suffix.

use krabka_blockstore::{ObjectStoreMetrics, TenantId};
pub use krabka_observability::service_metrics::{
    RouteLabel, RouteStatusLabel, SharedRegistry, StatusLabel, TenantLabel, metrics_router,
};
use krabka_observability::{
    compaction_metrics::CompactionMetrics,
    service_metrics::{
        IngestHelpText, IngestInstruments, IngestRequest, PipelineInstruments, QueryHelpText,
        QueryInstruments, QueryRequest, RequestOutcome, register_in_new_registry,
    },
    wal_consumer_metrics::WalConsumerMetrics,
    wal_produce::WalProduceMetrics,
};
use krabka_units::{ByteSize, Time};
use prometheus_client::{
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

mod service_metrics;

pub use self::service_metrics::ServiceMetrics;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::ObjectStoreOperation;
    use krabka_observability::service_metrics::encode_registry;
    use krabka_units::prelude::*;

    use super::{RouteStatusLabel, ServiceMetrics, TenantId, TenantLabel};

    fn tenant(id: &str) -> TenantId {
        TenantId::new(id).expect("a valid tenant id")
    }

    #[tokio::test]
    async fn registry_has_traces_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, kibibytes(1), 7, millis(10));
        m.record_ingest(false, ByteSize::ZERO, 0, millis(2));
        m.record_wal_append_failure();
        m.record_ingest_spans(&tenant("tenant-a"), 7);
        m.record_block_flushed();
        m.record_query("search", true, 0.05);
        m.record_query("trace_by_id", false, 0.2);
        // The shared bundles must land in this signal's registry.
        m.wal_consumer.record_partition_assigned("__wal", 2);
        m.wal_produce.record_batch_failure(1, 3);
        m.compaction.record_output(4);
        m.object_store.record_retry(ObjectStoreOperation::Get);

        let buf = encode_registry(&m.registry).await.unwrap();
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
            "krabka_traces_wal_consumer_partition_owned{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_traces_wal_partial_batch_appends_total 1",
            "krabka_traces_compaction_blocks_total 4",
            "krabka_traces_objstore_operation_retries_total{operation=\"get\"} 1",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"search\"",
            "tenant=\"tenant-a\"",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let m = ServiceMetrics::new();
        // A 4xx client error: error outcome, but NOT a WAL failure.
        m.record_ingest(false, ByteSize::ZERO, 0, Time::ZERO);
        assert!(m.ingest.wal_append_failures.get() == 0);
        // A produce failure: bump explicitly at the WAL error site.
        m.record_wal_append_failure();
        assert!(m.ingest.wal_append_failures.get() == 1);
    }

    #[test]
    fn ingest_spans_split_by_tenant_and_blocks_flushed_accumulate() {
        let m = ServiceMetrics::new();
        m.record_ingest_spans(&tenant("tenant-a"), 3);
        m.record_ingest_spans(&tenant("tenant-a"), 2);
        m.record_ingest_spans(&tenant("tenant-b"), 4);
        // A zero-span request must not create a tenant series.
        m.record_ingest_spans(&tenant("tenant-c"), 0);
        m.record_block_flushed();
        m.record_block_flushed();

        for (tenant, want) in [("tenant-a", 5), ("tenant-b", 4)] {
            check!(
                m.ingest_spans
                    .get_or_create(&TenantLabel {
                        tenant: tenant.into()
                    })
                    .get()
                    == want
            );
        }
        assert!(m.blocks_flushed.get() == 2);
    }

    // `record_query` takes its latency in seconds rather than as a `Time`,
    // so its outcome mapping is checked here as well as in the shared module.
    #[test]
    fn query_counters_split_by_route_and_status() {
        let m = ServiceMetrics::new();
        m.record_query("search", true, 0.01);
        m.record_query("search", false, 0.03);
        for (status, want) in [("ok", 1), ("error", 1)] {
            check!(
                m.query
                    .requests
                    .get_or_create(&RouteStatusLabel {
                        route: "search".into(),
                        status: status.into()
                    })
                    .get()
                    == want
            );
        }
    }
}
