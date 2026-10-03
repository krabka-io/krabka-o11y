use super::{
    Arc, Bytes, MAX_STATE_OBJECT_BYTES, ObjectStore, StorageAuditError, read_capped_object,
};

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
    read_capped_object(store, key, MAX_STATE_OBJECT_BYTES).await
}
