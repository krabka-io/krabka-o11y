use krabka_blockstore::{
    ConditionalUpdateRequirement, MeteredObjectStore, ObjectStoreAccess, ObjectStoreMetrics,
    verify_object_store_access,
};

use super::{
    Arc, ConfiguredObjectStore, LocalFileSystem, ObjectPath, ServiceConfig, ServiceConfigError,
    Url, parse_url_opts,
};

/// Builds the logs object store, wrapped so every request it serves is counted
/// in `metrics`.
///
/// The wrap happens here and nowhere else, so one decorator covers every
/// reader and writer in the role.
///
/// The store is probed with [`verify_object_store_access`] below its prefix
/// before it is returned. `access` names what the role does with the store. A
/// writer gets the full probe, so a store that lacks a semantic the role writes
/// against stops the role here, before it accepts any data. A reader gets the
/// read probe, which writes nothing, so a read-only credential is enough.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn build_configured_object_store(
    config: &ServiceConfig,
    metrics: ObjectStoreMetrics,
    access: ObjectStoreAccess,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    let Some(raw_url) = config.object_store_url.as_deref() else {
        return Ok(None);
    };
    let configured = open_configured_object_store(raw_url, metrics)?;
    verify_object_store_access(
        configured.store.as_ref(),
        &configured.prefix,
        access,
        ConditionalUpdateRequirement::for_object_store_url(raw_url),
    )
    .await?;
    Ok(Some(configured))
}

#[cfg_attr(test, mutants::skip)]
fn open_configured_object_store(
    raw_url: &str,
    metrics: ObjectStoreMetrics,
) -> Result<ConfiguredObjectStore, ServiceConfigError> {
    match Url::parse(raw_url) {
        Ok(url) if url.scheme() == "file" => {
            let path =
                url.to_file_path()
                    .map_err(|()| ServiceConfigError::InvalidObjectStoreUrl {
                        url: raw_url.to_string(),
                        reason: "file URL must map to a local filesystem path".to_string(),
                    })?;
            Ok(ConfiguredObjectStore {
                store: MeteredObjectStore::wrap(
                    Arc::new(LocalFileSystem::new_with_prefix(path)?),
                    metrics,
                ),
                prefix: ObjectPath::from(""),
            })
        }
        Ok(url) => {
            let (store, prefix) = parse_url_opts(&url, std::env::vars())?;
            Ok(ConfiguredObjectStore {
                store: MeteredObjectStore::wrap(Arc::from(store), metrics),
                prefix,
            })
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => Ok(ConfiguredObjectStore {
            store: MeteredObjectStore::wrap(
                Arc::new(LocalFileSystem::new_with_prefix(raw_url)?),
                metrics,
            ),
            prefix: ObjectPath::from(""),
        }),
        Err(error) => Err(ServiceConfigError::InvalidObjectStoreUrl {
            url: raw_url.to_string(),
            reason: error.to_string(),
        }),
    }
}
