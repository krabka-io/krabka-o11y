use super::*;

/// `head`s the block, rejects it above `max_bytes`, and hands back its on-disk
/// size for the Parquet reader.
///
/// The object store reports a raw `u64`, so the comparison is the one place
/// that lifts the size into a [`ByteSize`]. The rejection message still prints
/// whole bytes, so it reads the same for a caller-supplied cap and for the
/// default cap.
pub(crate) async fn head_within_cap(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<u64> {
    let meta = store.head(path).await?;
    tracing::Span::current().record("size", meta.size);
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {} bytes",
            meta.size,
            max_bytes.bytes_u64()
        )));
    }
    Ok(meta.size)
}
