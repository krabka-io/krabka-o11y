use super::{
    Arc, BlockStoreError, ByteSize, ByteSizeExt as _, Bytes, ObjectStore, ObjectStoreExt as _,
    Path, Result,
};

/// Reads one shard object, refusing to buffer more than `max_bytes` of it.
///
/// The cap is per shard rather than per index now that an index is many
/// objects, which is the tighter bound of the two: a corrupt or hostile object
/// under a shared prefix can no longer be as large as a whole fleet index and
/// still be read.
pub(crate) async fn read_index_shard(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    max_bytes: ByteSize,
) -> Result<Bytes> {
    match krabka_object_store::read_capped(store, path, max_bytes.bytes_u64()).await {
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(match error {
            krabka_object_store::ObjectStoreError::TooLarge {
                size, max_bytes, ..
            } => BlockStoreError::InvalidBlock(format!(
                "index shard `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            krabka_object_store::ObjectStoreError::Backend(message)
            | krabka_object_store::ObjectStoreError::InvalidConfig(message) => {
                BlockStoreError::ObjectStore(message)
            }
            krabka_object_store::ObjectStoreError::Io(error) => {
                BlockStoreError::ObjectStore(error.to_string())
            }
            not_found @ krabka_object_store::ObjectStoreError::NotFound(_) => {
                // A shard the listing named and the read could not find was
                // deleted between the two, which a concurrent save does. Report
                // it rather than answering from a partial index.
                match store.head(path).await {
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
