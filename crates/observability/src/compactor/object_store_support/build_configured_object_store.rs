use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics};

use super::{
    Arc, ConfiguredObjectStore, LocalFileSystem, ObjectPath, ServiceConfig, ServiceConfigError,
    Url, parse_url_opts,
};

/// Builds the logs object store, wrapped so every request it serves is counted
/// in `metrics`.
///
/// The wrap happens here and nowhere else, so one decorator covers every
/// reader and writer in the role.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn build_configured_object_store(
    config: &ServiceConfig,
    metrics: ObjectStoreMetrics,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    let Some(raw_url) = config.object_store_url.as_deref() else {
        return Ok(None);
    };

    match Url::parse(raw_url) {
        Ok(url) if url.scheme() == "file" => {
            let path =
                url.to_file_path()
                    .map_err(|()| ServiceConfigError::InvalidObjectStoreUrl {
                        url: raw_url.to_string(),
                        reason: "file URL must map to a local filesystem path".to_string(),
                    })?;
            Ok(Some(ConfiguredObjectStore {
                store: MeteredObjectStore::wrap(
                    Arc::new(LocalFileSystem::new_with_prefix(path)?),
                    metrics,
                ),
                prefix: ObjectPath::from(""),
            }))
        }
        Ok(url) => {
            let (store, prefix) = parse_url_opts(&url, std::env::vars())?;
            Ok(Some(ConfiguredObjectStore {
                store: MeteredObjectStore::wrap(Arc::from(store), metrics),
                prefix,
            }))
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => Ok(Some(ConfiguredObjectStore {
            store: MeteredObjectStore::wrap(
                Arc::new(LocalFileSystem::new_with_prefix(raw_url)?),
                metrics,
            ),
            prefix: ObjectPath::from(""),
        })),
        Err(error) => Err(ServiceConfigError::InvalidObjectStoreUrl {
            url: raw_url.to_string(),
            reason: error.to_string(),
        }),
    }
}
