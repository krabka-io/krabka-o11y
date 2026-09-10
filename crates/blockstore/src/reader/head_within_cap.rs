use super::{
    Arc, BlockStoreError, ByteSize, ByteSizeExt, ObjectMeta, ObjectStore, ObjectStoreExt, Path,
    Result, unreadable,
};

/// `head`s the block, rejects it above `max_bytes`, and hands back the object
/// as the store currently describes it.
///
/// The returned [`ObjectMeta`] carries the size the Parquet reader needs and
/// the `ETag`, version and modification time the footer cache validates against,
/// so the one `head` a read already paid for serves both.
///
/// A failed `head` is attributed to the block: a missing object is one block's
/// problem, and the caller has to be able to tell it from a store that is
/// down. An over-cap block is not — the block is readable and the store is
/// fine, and a query that quietly dropped a legitimately large block would
/// hide more data than it reported.
pub(crate) async fn head_within_cap(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<ObjectMeta> {
    let meta = store
        .head(path)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    tracing::Span::current().record("size", meta.size);
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {} bytes",
            meta.size,
            max_bytes.bytes_u64()
        )));
    }
    Ok(meta)
}
