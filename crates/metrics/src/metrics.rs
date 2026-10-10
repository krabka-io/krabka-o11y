//! Prometheus metrics for the metrics-subsystem ingest (distributor) role.
//!
//! This module mirrors the broker's `metrics` pattern. It holds a shared
//! `Registry` wrapped in `Arc<Mutex<…>>`, and a cheaply-`Clone` bundle of
//! metric handles that the ingest handlers clone and increment directly. The
//! registry prefix is `krabka_metrics`. `prometheus-client` appends `_total` to
//! counters at encode time, so this module registers counter names without the
//! suffix. [`krabka_observability::service_metrics`] holds the parts that every
//! signal shares.

use krabka_blockstore::ObjectStoreMetrics;
pub use krabka_observability::service_metrics::{
    SharedRegistry, StatusLabel, TenantLabel, metrics_router,
};
use krabka_observability::{
    RoleKind,
    compaction_metrics::CompactionMetrics,
    service_metrics::{
        IngestHelpText, IngestInstruments, IngestRequest, PipelineInstruments, RequestOutcome,
        RoleRegistry, register_for_role, register_in_new_registry,
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

pub use self::service_metrics::{
    INGEST_HELP, METRICS_PREFIX, ServiceMetrics, metrics_role_registry,
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::ObjectStoreOperation;
    use krabka_observability::service_metrics::encode_registry;
    use krabka_units::prelude::*;

    use super::ServiceMetrics;

    /// `record_blocks_compacted` skips a zero rather than adding it. The
    /// counter would be unchanged either way, so the skip is only observable
    /// as a difference from a non-zero call -- the test therefore records a
    /// real count, then a zero, and checks the total did not move.
    #[test]
    fn compacted_blocks_accumulate_and_a_zero_is_skipped() {
        let metrics = ServiceMetrics::new();
        check!(
            metrics.blocks_compacted.get() == 0,
            "a fresh counter is at zero"
        );

        metrics.record_blocks_compacted(3);
        check!(metrics.blocks_compacted.get() == 3);

        // Counts add rather than replace.
        metrics.record_blocks_compacted(4);
        check!(metrics.blocks_compacted.get() == 7, "added, not replaced");

        // A zero leaves the total where it was.
        metrics.record_blocks_compacted(0);
        check!(metrics.blocks_compacted.get() == 7, "zero changed nothing");

        // And a later real count still lands.
        metrics.record_blocks_compacted(1);
        check!(metrics.blocks_compacted.get() == 8);
    }

    #[tokio::test]
    async fn registry_has_metrics_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, kibibytes(1), 5, millis(12));
        m.record_ingest(false, ByteSize::ZERO, 0, millis(1));
        m.ingest.wal_append_failures.inc();
        m.record_ingest_series("tenant-a", 5);
        m.record_blocks_compacted(3);
        // The shared bundles must land in this signal's registry.
        m.wal_consumer.record_partition_assigned("__wal", 2);
        m.wal_produce.record_batch_failure(1, 3);
        m.compaction.record_output(4);
        m.object_store.record_retry(ObjectStoreOperation::Get);

        let buf = encode_registry(&m.registry).await.unwrap();
        for needle in [
            "krabka_metrics_ingest_requests_total",
            "krabka_metrics_ingest_bytes_total",
            "krabka_metrics_ingest_items_total",
            "krabka_metrics_ingest_duration_seconds",
            "krabka_metrics_wal_append_failures_total",
            "krabka_metrics_ingest_series_total",
            "krabka_metrics_blocks_compacted_total",
            "krabka_metrics_wal_consumer_partition_owned{topic=\"__wal\",partition=\"2\"} 1",
            "krabka_metrics_wal_partial_batch_appends_total 1",
            "krabka_metrics_compaction_blocks_total 4",
            "krabka_metrics_objstore_operation_retries_total{operation=\"get\"} 1",
            "status=\"ok\"",
            "status=\"error\"",
            "tenant=\"tenant-a\"",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[test]
    fn record_ingest_does_not_touch_wal_failures() {
        let m = ServiceMetrics::new();
        // An error outcome must NOT bump wal_append_failures — that is reserved
        // for actual WAL/produce errors, incremented at the append site.
        m.record_ingest(false, ByteSize::ZERO, 0, Time::ZERO);
        assert!(m.ingest.wal_append_failures.get() == 0);
    }
}
