use super::{
    Arc, BlockStoreError, ByteSize, ByteSizeExt as _, Bytes, ObjectStore, ObjectStoreExt as _,
    Path, Result,
};

/// Outcome of reading a snapshot object.
///
/// An absent object is not always a failure: a first save starts from nothing,
/// and a merge base can be pruned out from under a reader. The error a strict
/// reader should report travels with the absence so that neither caller has to
/// re-derive it.
pub(crate) enum IndexSnapshotBytes {
    Present(Bytes),
    Absent(BlockStoreError),
}

/// Reads a snapshot object, capped at `max_bytes`.
///
/// `label` names the snapshot flavour in the error text.
pub(crate) async fn read_index_snapshot_bytes(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    max_bytes: ByteSize,
    label: &str,
) -> Result<IndexSnapshotBytes> {
    match krabka_object_store::read_capped(store, path, max_bytes.bytes_u64()).await {
        Ok(bytes) => Ok(IndexSnapshotBytes::Present(bytes)),
        Err(error) => Err(match error {
            krabka_object_store::ObjectStoreError::TooLarge {
                size, max_bytes, ..
            } => BlockStoreError::InvalidBlock(format!(
                "{label} `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            krabka_object_store::ObjectStoreError::Backend(message)
            | krabka_object_store::ObjectStoreError::InvalidConfig(message) => {
                BlockStoreError::ObjectStore(message)
            }
            krabka_object_store::ObjectStoreError::Io(error) => {
                BlockStoreError::ObjectStore(error.to_string())
            }
            not_found @ krabka_object_store::ObjectStoreError::NotFound(_) => {
                return match store.head(path).await {
                    // The object is there after all, so the read is the
                    // failure, not the absence.
                    Ok(_) => Err(BlockStoreError::ObjectStore(not_found.to_string())),
                    Err(missing @ object_store::Error::NotFound { .. }) => {
                        Ok(IndexSnapshotBytes::Absent(BlockStoreError::ObjectStore(
                            missing.to_string(),
                        )))
                    }
                    Err(error) => Err(BlockStoreError::ObjectStore(error.to_string())),
                };
            }
            // Write-side variants: `read_capped` cannot raise them, but
            // they are part of the enum, so surface them like any other
            // backend failure rather than widening the read path.
            conflict @ (krabka_object_store::ObjectStoreError::AlreadyExists(_)
            | krabka_object_store::ObjectStoreError::Precondition { .. }) => {
                BlockStoreError::ObjectStore(conflict.to_string())
            }
        }),
    }
}
