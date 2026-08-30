use super::{
    ObjectPath, QuerierIndexSource, ServiceConfig, ServiceConfigError,
    effective_object_store_prefix,
};

pub(crate) fn querier_object_store_prefix(
    config: &ServiceConfig,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<Option<ObjectPath>, ServiceConfigError> {
    match config.querier_index_source {
        QuerierIndexSource::LocalManifest => Ok(None),
        QuerierIndexSource::TenantObjectStoreManifest
        | QuerierIndexSource::TenantObjectStoreShards => {
            let prefix =
                config
                    .index_prefix
                    .as_deref()
                    .ok_or(ServiceConfigError::MissingIndexPrefix {
                        index_source: config.querier_index_source,
                    })?;
            Ok(Some(effective_object_store_prefix(
                object_store_prefix,
                prefix,
            )))
        }
    }
}
