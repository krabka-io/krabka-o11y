use super::{Arc, ObjectStore, Path, CompactionRetentionError, ObjectStoreExt};

pub(crate) async fn delete_if_exists(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
) -> Result<bool, CompactionRetentionError> {
    match store.delete(location).await {
        Ok(()) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(CompactionRetentionError::ObjectStore(error.to_string())),
    }
}
