use std::time::Instant;

use krabka_units::{Time, convert::TimeExt as _};

use super::{
    BlockDescriptor, CompactorRunError, Instrument, KafkaWalRecord, LogWalConsumer, ObjectPath,
    ObjectStore, SharedLogDeleteRequests, TenantCompactionIndexCache,
    compact_polled_kafka_wal_records_inner, set_remote_parent_from_wal_records,
};
use crate::compaction_metrics::CompactionMetrics;

/// Compacts one polled WAL batch and records the pass in `metrics`.
///
/// This is where every compactor entry point meets, so it is the one place the
/// compaction instruments have to move. A batch with no records returns before
/// the timer starts: nothing was compacted, and counting it as a pass would
/// make an idle compactor look busy. The poll that returned nothing is already
/// counted by the WAL consumer instruments.
pub(crate) async fn compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
    metrics: &CompactionMetrics,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // ONE consumer span per poll batch: stitch it onto the ingest trace via the
    // `traceparent` a producer injected in `build_kafka_wal_record`. The first
    // record carrying a trace context is representative of the batch.
    let span = tracing::info_span!(
        "logs_compaction",
        otel.kind = "consumer",
        krabka.wal.records = records.len(),
    );
    set_remote_parent_from_wal_records(&span, &records);

    let started = Instant::now();
    let compacted = compact_polled_kafka_wal_records_inner(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .instrument(span)
    .await;
    metrics.record_run(compacted.is_ok(), Time::from_std(started.elapsed()));
    if let Ok(descriptors) = &compacted {
        metrics.record_output(descriptors.len() as u64);
    }
    compacted
}
