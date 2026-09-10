use super::{
    Arc, BlockStoreError, ByteSize, ByteSizeExt as _, Bytes, ObjectStore, ObjectStoreExt as _,
    Path, Result,
};

/// Reads one shard payload, refusing to buffer more than `max_bytes` of it.
///
/// The cap is per payload rather than per index, which is the tighter bound of
/// the two: a corrupt or hostile object under a shared prefix can no longer be
/// as large as a whole fleet index and still be read.
pub(crate) async fn read_shard_payload(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Bytes> {
    let path = Path::from(object_key);
    match krabka_object_store::read_capped(store, &path, max_bytes.bytes_u64()).await {
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(match error {
            krabka_object_store::ObjectStoreError::TooLarge {
                size, max_bytes, ..
            } => BlockStoreError::InvalidBlock(format!(
                "index shard payload `{object_key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            krabka_object_store::ObjectStoreError::Backend(message)
            | krabka_object_store::ObjectStoreError::InvalidConfig(message) => {
                BlockStoreError::ObjectStore(message)
            }
            krabka_object_store::ObjectStoreError::Io(error) => {
                BlockStoreError::ObjectStore(error.to_string())
            }
            not_found @ krabka_object_store::ObjectStoreError::NotFound(_) => {
                // A payload a live manifest names is not allowed to be
                // missing. The orphan sweep leaves anything a retained
                // manifest names alone, so this is a torn write or an outside
                // deletion, and answering from a partial index would hide it.
                match store.head(&path).await {
                    Ok(_) => BlockStoreError::ObjectStore(not_found.to_string()),
                    Err(missing) => BlockStoreError::ObjectStore(missing.to_string()),
                }
            }
            // Write-side variants: `read_capped` cannot raise them, but they
            // are part of the enum, so surface them like any other backend
            // failure rather than widening the read path.
            conflict @ (krabka_object_store::ObjectStoreError::AlreadyExists(_)
            | krabka_object_store::ObjectStoreError::Precondition { .. }) => {
                BlockStoreError::ObjectStore(conflict.to_string())
            }
        }),
    }
}
