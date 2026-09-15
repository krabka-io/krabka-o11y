use super::{
    BlockKey, BlockStoreError, File, LogRow, ParquetRecordBatchReaderBuilder, Path, batch_to_rows,
    block_path, instrument,
};

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
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(BlockStoreError::UnsupportedBlockFormat)?;
    let reader = builder.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_to_rows(&batch?)?);
    }
    Ok(rows)
}
