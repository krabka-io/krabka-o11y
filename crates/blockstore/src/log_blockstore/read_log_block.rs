use super::{BlockKey, BlockStoreError, File, LogRow, Path, block_path, instrument};

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn read_log_block(
    root: impl AsRef<Path>,
    key: &BlockKey,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let file = File::open(block_path(root, key))?;
    super::read_log_block_from_reader(file)
}
