use super::*;

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn register_log_blocks_from_object_store(
    ctx: &SessionContext,
    table_name: &str,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    blocks: &[BlockDescriptor],
) -> Result<(), BlockStoreError> {
    let table = Arc::new(LogBlockTableProvider::try_new_object_store(
        store, prefix, blocks,
    )?);
    ctx.register_table(table_name, table)?;
    Ok(())
}
