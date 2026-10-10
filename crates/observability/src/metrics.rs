//! Logs-service Prometheus metrics.
//!
//! This metric spec is the same across the LGTM observability services, and
//! [`crate::service_metrics`] holds the parts they share. It is a
//! `prometheus-client` registry with prefix `krabka_logs`, wrapped in
//! `Arc<Mutex<…>>` so the `/metrics` exporter can lock it. The cheaply
//! cloneable [`ServiceMetrics`] hands out counter and histogram handles, and
//! the ingest (distributor) and query (querier) handlers increment those
//! handles directly.
//!
//! `prometheus-client` auto-appends `_total` to counters at encode time, so
//! counter names are registered WITHOUT the suffix.

use krabka_blockstore::ObjectStoreMetrics;
use krabka_units::{ByteSize, Time, convert::TimeExt};
use prometheus_client::{
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

pub use crate::service_metrics::{
    RouteLabel, RouteStatusLabel, SharedRegistry, StatusLabel, TenantLabel, metrics_router,
};
use crate::{
    compaction_metrics::CompactionMetrics,
    service_metrics::{
        IngestHelpText, IngestInstruments, IngestRequest, PipelineInstruments, QueryHelpText,
        QueryInstruments, QueryRequest, RequestOutcome, register_in_new_registry,
    },
    wal_consumer_metrics::WalConsumerMetrics,
    wal_produce::WalProduceMetrics,
};

mod service_metrics;

pub use self::service_metrics::ServiceMetrics;

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_blockstore::ObjectStoreOperation;
    use krabka_units::{bytes, millis};

    use super::{ServiceMetrics, TenantLabel};
    use crate::service_metrics::encode_registry;

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
        // The shared bundles must land in this signal's registry.
        m.wal_consumer.record_partition_assigned("__wal", 2);
        m.wal_produce.record_batch_failure(1, 3);
        m.compaction.record_output(4);
        m.object_store.record_retry(ObjectStoreOperation::Get);

        let buf = encode_registry(&m.registry).await.unwrap();
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
            "krabka_logs_wal_consumer_partition_owned{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_logs_wal_partial_batch_appends_total 1",
            "krabka_logs_compaction_blocks_total 4",
            "krabka_logs_objstore_operation_retries_total{operation=\"get\"} 1",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"query\"",
            "route=\"query_range\"",
            "tenant=\"demo\"",
        ] {
            check!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[test]
    fn ingest_lines_skip_a_zero_and_accumulate_per_tenant() {
        let m = ServiceMetrics::new();
        m.record_ingest_lines("demo", 3);
        m.record_ingest_lines("demo", 0);
        m.record_ingest_lines("demo", 4);
        m.record_block_written();
        check!(
            m.ingest_lines
                .get_or_create(&TenantLabel {
                    tenant: "demo".into()
                })
                .get()
                == 7
        );
        check!(m.blocks_written.get() == 1);
    }
}
