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
        QueryInstruments, QueryRequest, register_in_new_registry,
    },
    wal_consumer_metrics::WalConsumerMetrics,
    wal_produce::WalProduceMetrics,
};

mod service_metrics;

pub use self::service_metrics::ServiceMetrics;

#[cfg(test)]
#[path = "../tests/support/pipeline_bundle_metrics.rs"]
mod pipeline_bundle_metrics;

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::{bytes, millis};

    use super::{
        IngestRequest, QueryRequest, ServiceMetrics, TenantLabel,
        pipeline_bundle_metrics::{
            check_pipeline_bundles_exported, record_one_event_per_pipeline_bundle,
        },
    };
    use crate::service_metrics::{RequestOutcome, encode_registry};

    #[tokio::test]
    async fn registry_has_logs_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(IngestRequest {
            outcome: RequestOutcome::Ok,
            body: bytes(1_024),
            items: 7,
            elapsed: millis(10),
        });
        m.record_ingest(IngestRequest {
            outcome: RequestOutcome::Error,
            body: bytes(0),
            items: 0,
            elapsed: millis(2),
        });
        m.record_wal_append_failure();
        m.record_ingest_lines("demo", 7);
        m.record_block_written();
        m.record_query(QueryRequest {
            route: "query",
            outcome: RequestOutcome::Ok,
            elapsed: millis(50),
        });
        m.record_query(QueryRequest {
            route: "query_range",
            outcome: RequestOutcome::Error,
            elapsed: millis(200),
        });
        // The shared bundles must land in this signal's registry.
        record_one_event_per_pipeline_bundle(&m);

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
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"query\"",
            "route=\"query_range\"",
            "tenant=\"demo\"",
        ] {
            check!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        check_pipeline_bundles_exported(&buf, "krabka_logs");
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
