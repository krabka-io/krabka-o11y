use super::*;

#[instrument(
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition, rows = rows.len()),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn write_log_block(
    root: impl AsRef<Path>,
    key: &BlockKey,
    mut rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    validate_rows(key, &rows)?;
    rows.sort_by_key(|row| (row.series_fingerprint, row.timestamp_ns));

    let path = block_path(root, key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let schema = log_block_schema();
    let batch = rows_to_batch(&rows, Arc::clone(&schema))?;
    let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let size = ByteSize::from_bytes(fs::metadata(&path)?.len());

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        size,
    ))
}
