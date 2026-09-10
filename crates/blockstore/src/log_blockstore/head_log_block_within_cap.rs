use super::{BlockStoreError, ByteSize, ByteSizeExt, ObjectPath, ObjectStore, ObjectStoreExt};

/// `head`s one planned block and rejects it above `max_bytes`.
///
/// The object store reports a raw `u64`, so this is the one place that lifts a
/// block's on-disk size into a [`ByteSize`]. The message prints whole bytes, so
/// it reads the same however the cap was chosen.
pub(crate) async fn head_log_block_within_cap(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    max_bytes: ByteSize,
) -> Result<(), BlockStoreError> {
    let meta = store.head(path).await?;
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::LogBlockTooLarge {
            object_key: path.to_string(),
            size_bytes: meta.size,
            max_bytes: max_bytes.bytes_u64(),
        });
    }
    Ok(())
}
