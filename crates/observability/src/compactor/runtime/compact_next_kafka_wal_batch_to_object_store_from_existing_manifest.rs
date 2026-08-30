use super::*;

pub(crate) async fn compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .await
}
