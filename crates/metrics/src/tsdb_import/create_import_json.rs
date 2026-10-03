use super::{ObjectStore, Path, PutMode, PutOptions, PutPayload, Serialize, TsdbPublishError};

/// Creates `value` as JSON at `key`, and answers `false` if the key exists.
pub async fn create_import_json<T: Serialize>(
    store: &dyn ObjectStore,
    key: &Path,
    value: &T,
) -> Result<bool, TsdbPublishError> {
    let bytes = serde_json::to_vec(value).map_err(|error| TsdbPublishError::InvalidRecord {
        key: key.to_string(),
        reason: error.to_string(),
    })?;
    match store
        .put_opts(
            key,
            PutPayload::from(bytes),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(source) => Err(TsdbPublishError::ObjectStore {
            key: key.to_string(),
            source,
        }),
    }
}
