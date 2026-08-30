use super::{
    BlockDescriptor, BlockIndex, CompactionOffsetCommitter, KafkaWalCompactionError,
    KafkaWalRecord, LabelIndex, ObjectPath, ObjectStore, compact_wal_records_to_object_store,
    decode_kafka_wal_record_envelope,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_kafka_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<KafkaWalRecord>,
) -> Result<BlockDescriptor, KafkaWalCompactionError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(compact_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        decoded,
    )
    .await?)
}
