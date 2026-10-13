use super::{
    BlockDescriptor, CompactorRunError, NextKafkaBatch,
    compact_next_kafka_wal_batch_to_object_store_from_existing_manifest,
    materialize_log_deletes_before_compaction,
};

pub(crate) async fn materialize_deletes_then_compact_next_kafka_wal_batch(
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
    materialize_log_deletes_before_compaction(
        store,
        prefix,
        delete_requests,
        tenant_indexes,
        metrics,
    )
    .await?;
    compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(NextKafkaBatch {
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
        metrics,
    })
    .await
}
