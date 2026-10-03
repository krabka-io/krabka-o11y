use super::{
    DeserializeOwned, ObjectStore, ObjectStoreExt, Path, TsdbImportRecord, TsdbPublishError,
};

/// Reads a versioned import JSON object, or `None` if the key is absent.
///
/// The version is checked before the rest is decoded, so a record that a
/// newer build wrote fails with
/// [`TsdbPublishError::UnsupportedRecordVersion`] and changes nothing.
pub async fn read_import_json<T: DeserializeOwned>(
    store: &dyn ObjectStore,
    key: &Path,
) -> Result<Option<T>, TsdbPublishError> {
    let object_error = |source| TsdbPublishError::ObjectStore {
        key: key.to_string(),
        source,
    };
    let bytes = match store.get(key).await {
        Ok(object) => object.bytes().await.map_err(object_error)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(source) => return Err(object_error(source)),
    };
    let invalid = |reason: String| TsdbPublishError::InvalidRecord {
        key: key.to_string(),
        reason,
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| invalid(error.to_string()))?;
    let version = value
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .filter(|version| *version > 0)
        .ok_or_else(|| invalid("the version is missing or 0".to_owned()))?;
    if version > u64::from(TsdbImportRecord::VERSION) {
        return Err(TsdbPublishError::UnsupportedRecordVersion {
            key: key.to_string(),
            version: u32::try_from(version).unwrap_or(u32::MAX),
            supported: TsdbImportRecord::VERSION,
        });
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|error| invalid(error.to_string()))
}
