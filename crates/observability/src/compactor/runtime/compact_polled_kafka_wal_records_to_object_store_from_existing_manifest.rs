use super::{
    BlockDescriptor, CompactorRunError, Instrument, KafkaWalRecord, LogWalConsumer, ObjectPath,
    ObjectStore, SharedLogDeleteRequests, TenantCompactionIndexCache,
    compact_polled_kafka_wal_records_inner, set_remote_parent_from_wal_records,
};

pub(crate) async fn compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
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

    compact_polled_kafka_wal_records_inner(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .instrument(span)
    .await
}
