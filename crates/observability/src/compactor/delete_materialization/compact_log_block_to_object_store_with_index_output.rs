use super::{
    BlockDescriptor, BlockIndex, BlockKey, BlockStoreError, LabelIndex, LogCompactionIndexOutput,
    LogRow, ObjectPath, ObjectStore, write_log_block_to_object_store,
    write_tenant_compaction_indexes_to_object_store,
};

pub(crate) async fn compact_log_block_to_object_store_with_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
    index_output: LogCompactionIndexOutput,
) -> Result<BlockDescriptor, BlockStoreError> {
    let descriptor = write_log_block_to_object_store(store, prefix, key, rows).await?;
    block_index.insert(descriptor.clone());
    write_tenant_compaction_indexes_to_object_store(
        store,
        prefix,
        &key.tenant,
        &descriptor,
        label_index,
        block_index,
        index_output,
    )
    .await?;
    Ok(descriptor)
}
