use super::{
    BTreeMap, BlockDescriptor, BlockIndex, CompactionError, CompactorRunError, KafkaWalRecord,
    LabelIndex, LastCompactedPosition, LogCompactionIndexOutput, LogWalConsumer, ObjectPath,
    ObjectStore, Offset, PartitionIndex, SharedLogDeleteRequests, TenantCompactionIndexCache,
    WalPosition, active_log_delete_filters_from_requests,
    compact_wal_records_to_object_store_with_delete_filters_and_index_output,
    decode_kafka_wal_record_envelope, wal_compaction_chunks, wal_record_time_range,
};

pub(crate) async fn compact_polled_kafka_wal_records_inner(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;
    let mut descriptors = Vec::new();
    let mut commit_positions: BTreeMap<PartitionIndex, Offset> = BTreeMap::new();

    for chunk in wal_compaction_chunks(decoded) {
        let tenant = chunk
            .first()
            .ok_or(CompactionError::EmptyWalBatch)?
            .tenant
            .clone();
        let (label_index, block_index) = tenant_indexes
            .entry(tenant.clone())
            .or_insert_with(|| (LabelIndex::default(), BlockIndex::default()));
        let mut committer = LastCompactedPosition::default();
        let time_range = wal_record_time_range(&chunk)?;
        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, &tenant, time_range)?;
        let descriptor = compact_wal_records_to_object_store_with_delete_filters_and_index_output(
            store,
            prefix,
            label_index,
            block_index,
            &mut committer,
            chunk,
            (&delete_filters, LogCompactionIndexOutput::ShardManifests),
        )
        .await?;
        let position = committer
            .position
            .ok_or(CompactorRunError::MissingCommitPosition)?;
        commit_positions
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
        if let Some(descriptor) = descriptor {
            descriptors.push(descriptor);
        }
    }

    for (partition, offset) in commit_positions {
        consumer
            .commit_compacted(WalPosition { partition, offset })
            .await?;
    }

    Ok(descriptors)
}
