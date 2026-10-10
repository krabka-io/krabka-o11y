//! The pipeline bundles every signal's metrics registry must export.
//!
//! Each signal's `ServiceMetrics` carries the WAL-consumer, WAL-produce,
//! compaction and object-store bundles under its own prefix. The logs crate's
//! unit tests and the `krabka-traces` crate's reach this file with `#[path]`
//! from their `metrics` module, whose `ServiceMetrics` it uses.

use krabka_blockstore::ObjectStoreOperation;

use super::ServiceMetrics;

/// Records one event on each pipeline bundle of `metrics`, the events that
/// [`check_pipeline_bundles_exported`] looks for.
pub(super) fn record_one_event_per_pipeline_bundle(metrics: &ServiceMetrics) {
    metrics.wal_consumer.record_partition_assigned("__wal", 2);
    metrics.wal_produce.record_batch_failure(1, 3);
    metrics.compaction.record_output(4);
    metrics.object_store.record_retry(ObjectStoreOperation::Get);
}

/// Checks that `exposition`, a registry encoded after
/// [`record_one_event_per_pipeline_bundle`], holds each bundle's sample under
/// the signal's metric `prefix`, such as `krabka_logs`.
pub(super) fn check_pipeline_bundles_exported(exposition: &str, prefix: &str) {
    for sample in [
        "wal_consumer_partition_owned{topic=\"__wal\",partition=\"2\"} 1",
        "wal_partial_batch_appends_total 1",
        "compaction_blocks_total 4",
        "objstore_operation_retries_total{operation=\"get\"} 1",
    ] {
        let needle = format!("{prefix}_{sample}");
        assert2::check!(
            exposition.contains(&needle),
            "missing {needle} in:\n{exposition}"
        );
    }
}
