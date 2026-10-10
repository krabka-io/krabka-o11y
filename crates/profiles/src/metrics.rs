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

use krabka_blockstore::ObjectStoreMetrics;
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
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
};
use prometheus_client::{
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

use crate::ids::{IngestBytes, IngestItems};

mod service_metrics;

pub use self::service_metrics::ServiceMetrics;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::ObjectStoreOperation;
    use krabka_observability::service_metrics::encode_registry;
    use krabka_units::millis;

    use super::{IngestBytes, IngestItems, ServiceMetrics, StatusLabel};

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
        m.debuginfo_upload_retries.inc();
        m.debuginfo_upload_timeouts.inc();
        m.record_symbolizer_cache(true);
        m.record_symbolizer_cache(false);
        // The shared bundles must land in this signal's registry.
        m.wal_consumer.record_partition_assigned("__wal", 2);
        m.wal_produce.record_batch_failure(1, 3);
        m.compaction.record_output(4);
        m.object_store.record_retry(ObjectStoreOperation::Get);

        let buf = encode_registry(&m.registry).await.unwrap();
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
            "krabka_profiles_debuginfo_upload_retries_total",
            "krabka_profiles_debuginfo_upload_timeouts_total",
            "krabka_profiles_symbolizer_cache_requests_total",
            "krabka_profiles_wal_consumer_partition_owned{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_profiles_wal_partial_batch_appends_total 1",
            "krabka_profiles_compaction_blocks_total 4",
            "krabka_profiles_objstore_operation_retries_total{operation=\"get\"} 1",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        for label in [
            "tenant=\"tenant-a\"",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"select_series\"",
            "status=\"hit\"",
            "status=\"miss\"",
        ] {
            check!(buf.contains(label), "label {label} missing");
        }
    }

    #[test]
    fn record_ingest_adds_positive_bytes_and_items() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, IngestBytes(1024), IngestItems(3), millis(12));

        // A positive body/item count must flow through to the cumulative
        // counters, so a dropped or zeroed `inc_by` leaves these at zero.
        check!(m.ingest.bytes.get() == 1024);
        check!(m.ingest.items.get() == 3);
    }

    /// The request counter is split by outcome, so a swapped status label
    /// would report every failure as a success and vice versa. Both are
    /// recorded here and each is checked to have moved only its own series.
    #[test]
    fn ingest_requests_are_counted_under_their_own_outcome() {
        let m = ServiceMetrics::new();
        let count = |status: &str| {
            m.ingest
                .requests
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
        assert!(m.ingest.wal_append_failures.get() == 0);
        // Only the explicit WAL-failure call does.
        m.record_wal_append_failure();
        assert!(m.ingest.wal_append_failures.get() == 1);
    }
}
