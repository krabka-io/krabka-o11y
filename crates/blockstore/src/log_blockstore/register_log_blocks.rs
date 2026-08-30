use super::{Arc, BlockDescriptor, BlockStoreError, LogBlockTableProvider, Path, SessionContext};

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn register_log_blocks(
    ctx: &SessionContext,
    table_name: &str,
    root: impl AsRef<Path>,
    blocks: &[BlockDescriptor],
) -> Result<(), BlockStoreError> {
    let table = Arc::new(LogBlockTableProvider::try_new(root, blocks)?);
    ctx.register_table(table_name, table)?;
    Ok(())
}
