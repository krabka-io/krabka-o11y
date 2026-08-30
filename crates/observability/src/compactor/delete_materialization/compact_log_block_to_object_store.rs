use super::{
    BlockDescriptor, BlockIndex, BlockKey, BlockStoreError, LabelIndex, LogCompactionIndexOutput,
    LogRow, ObjectPath, ObjectStore, compact_log_block_to_object_store_with_index_output,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        key,
        label_index,
        block_index,
        rows,
        LogCompactionIndexOutput::FullManifestAndShardCatalog,
    )
    .await
}
