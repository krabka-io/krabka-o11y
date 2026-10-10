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
    match krabka_object_store::v013::read_capped(store, path, max_bytes.bytes_u64()).await {
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(match error {
            krabka_object_store::v013::ObjectStoreError::TooLarge {
                size, max_bytes, ..
            } => BlockStoreError::InvalidBlock(format!(
                "index shard `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            other => capped_read_error(store, path, other).await,
        }),
    }
}

/// Maps a `read_capped` failure other than `TooLarge` onto the blockstore
/// error the index readers report.
///
/// A missing object is re-checked with a `head`: an index object that a
/// listing or a live manifest names is not allowed to vanish, whether a
/// concurrent save deleted it or a torn write or outside deletion did, and
/// answering from a partial index would hide that.
pub(crate) async fn capped_read_error(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    error: krabka_object_store::v013::ObjectStoreError,
) -> BlockStoreError {
    match error {
        krabka_object_store::v013::ObjectStoreError::Backend(message)
        | krabka_object_store::v013::ObjectStoreError::InvalidConfig(message) => {
            BlockStoreError::ObjectStore(message)
        }
        krabka_object_store::v013::ObjectStoreError::Io(error) => {
            BlockStoreError::ObjectStore(error.to_string())
        }
        not_found @ krabka_object_store::v013::ObjectStoreError::NotFound(_) => {
            match store.head(path).await {
                Ok(_) => BlockStoreError::ObjectStore(not_found.to_string()),
                Err(missing) => BlockStoreError::ObjectStore(missing.to_string()),
            }
        }
        // Write-side variants: `read_capped` cannot raise them, but they are
        // part of the enum, so surface them like any other backend failure
        // rather than widening the read path. `TooLarge` reaches here only if
        // a caller forwards it, and is a backend failure like the rest.
        other @ (krabka_object_store::v013::ObjectStoreError::AlreadyExists(_)
        | krabka_object_store::v013::ObjectStoreError::Precondition { .. }
        | krabka_object_store::v013::ObjectStoreError::TooLarge { .. }) => {
            BlockStoreError::ObjectStore(other.to_string())
        }
    }
}
