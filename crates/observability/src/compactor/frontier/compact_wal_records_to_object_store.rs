use super::{
    BlockDescriptor, BlockIndex, CompactionError, CompactionOffsetCommitter, LabelIndex,
    LogCompactionIndexOutput, ObjectPath, ObjectStore, WalLogRecord,
    compact_wal_records_to_object_store_with_delete_filters_and_index_output,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
) -> Result<BlockDescriptor, CompactionError> {
    compact_wal_records_to_object_store_with_delete_filters_and_index_output(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        records,
        (&[], LogCompactionIndexOutput::FullManifestAndShardCatalog),
    )
    .await?
    .ok_or(CompactionError::AllRowsDeleted)
}
