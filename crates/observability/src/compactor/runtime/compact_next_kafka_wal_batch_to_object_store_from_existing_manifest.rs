use super::{
    BlockDescriptor, CompactorRunError, NextKafkaBatch,
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest,
};

pub(crate) async fn compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
    batch: NextKafkaBatch<'_>,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let NextKafkaBatch {
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
        metrics,
    } = batch;
    let records = consumer.poll(poll_timeout).await?;
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
        metrics,
    )
    .await
}
