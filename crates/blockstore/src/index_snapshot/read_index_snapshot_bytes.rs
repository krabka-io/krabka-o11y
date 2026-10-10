use super::{
    Arc, BlockStoreError, ByteSize, ByteSizeExt as _, Bytes, ObjectStore, ObjectStoreExt as _,
    Path, Result,
};
use crate::index::{CappedObject, capped_read_error, oversized_object_error};

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
    let error =
        match krabka_object_store::v013::read_capped(store, path, max_bytes.bytes_u64()).await {
            Ok(bytes) => return Ok(IndexSnapshotBytes::Present(bytes)),
            Err(error) => error,
        };
    if let Some(oversized) = oversized_object_error(CappedObject { label, name: path }, &error) {
        return Err(oversized);
    }
    if let krabka_object_store::v013::ObjectStoreError::NotFound(_) = error {
        return match store.head(path).await {
            // The object is there after all, so the read is the failure, not
            // the absence.
            Ok(_) => Err(BlockStoreError::ObjectStore(error.to_string())),
            Err(missing @ object_store::Error::NotFound { .. }) => Ok(IndexSnapshotBytes::Absent(
                BlockStoreError::ObjectStore(missing.to_string()),
            )),
            Err(error) => Err(BlockStoreError::ObjectStore(error.to_string())),
        };
    }
    Err(capped_read_error(store, path, error).await)
}
