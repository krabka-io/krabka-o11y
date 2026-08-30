use super::*;

pub(crate) async fn materialize_deletes_then_compact_next_kafka_wal_batch(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    materialize_log_deletes_before_compaction(store, prefix, delete_requests, tenant_indexes)
        .await?;
    compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
    )
    .await
}
