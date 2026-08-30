use super::{
    BlockKey, BlockStoreError, LogRow, ObjectPath, ObjectStore, ObjectStoreExt, instrument,
    log_block_object_path, read_log_block_from_reader,
};

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_log_block_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let bytes = store
        .get(&log_block_object_path(prefix, key))
        .await?
        .bytes()
        .await?;
    read_log_block_from_reader(bytes)
}
