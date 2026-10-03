use super::{ObjectStore, ObjectStoreExt, Path, PutPayload, Serialize, TsdbPublishError};

/// Writes `value` as JSON at `key`, and replaces an object at the key.
pub async fn put_import_json<T: Serialize>(
    store: &dyn ObjectStore,
    key: &Path,
    value: &T,
) -> Result<(), TsdbPublishError> {
    let bytes = serde_json::to_vec(value).map_err(|error| TsdbPublishError::InvalidRecord {
        key: key.to_string(),
        reason: error.to_string(),
    })?;
    store
        .put(key, PutPayload::from(bytes))
        .await
        .map_err(|source| TsdbPublishError::ObjectStore {
            key: key.to_string(),
            source,
        })?;
    Ok(())
}
