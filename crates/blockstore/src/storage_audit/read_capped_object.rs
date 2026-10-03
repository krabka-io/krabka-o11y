use super::{Arc, ByteSize, ByteSizeExt, Bytes, ObjectStore, Path, StorageAuditError};

/// Reads an object of at most `max_bytes`, or `None` when it is gone.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence, or the object is larger than `max_bytes`.
pub async fn read_capped_object(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
) -> Result<Option<Bytes>, StorageAuditError> {
    match krabka_object_store::v013::read_capped(store, &Path::from(key), max_bytes.bytes_u64())
        .await
    {
        Ok(bytes) => Ok(Some(bytes)),
        Err(krabka_object_store::v013::ObjectStoreError::NotFound(_)) => Ok(None),
        Err(error) => Err(StorageAuditError::ObjectStore(error.to_string())),
    }
}
