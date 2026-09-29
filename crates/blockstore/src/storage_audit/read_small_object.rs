use super::{Arc, Bytes, MAX_STATE_OBJECT_BYTES, ObjectStore, Path, StorageAuditError};

/// Reads a small state object, or `None` when it is gone.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence, or the object is larger than
/// [`MAX_STATE_OBJECT_BYTES`].
pub async fn read_small_object(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<Bytes>, StorageAuditError> {
    match krabka_object_store::read_capped(store, &Path::from(key), MAX_STATE_OBJECT_BYTES).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(krabka_object_store::ObjectStoreError::NotFound(_)) => Ok(None),
        Err(error) => Err(StorageAuditError::ObjectStore(error.to_string())),
    }
}
