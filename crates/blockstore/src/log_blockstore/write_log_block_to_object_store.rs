use super::*;

#[instrument(
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition, rows = rows.len(), size = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    mut rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    validate_rows(key, &rows)?;
    rows.sort_by_key(|row| (row.series_fingerprint, row.timestamp_ns));

    let payload = encode_log_block(&rows)?;
    let size_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    tracing::Span::current().record("size", size_bytes);
    store
        .put(&log_block_object_path(prefix, key), payload.into())
        .await?;

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        ByteSize::from_bytes(size_bytes),
    ))
}
