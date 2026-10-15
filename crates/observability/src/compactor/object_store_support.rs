use krabka_blockstore::{
    ConditionalUpdateRequirement, MeteredObjectStore, ObjectStoreAccess, ObjectStoreMetrics,
    verify_object_store_access,
};

use crate::{
    Arc, CompactionFrontierRefreshSource, ConfiguredObjectStore, LocalFileSystem,
    LogDeleteRequestStoreError, ObjectPath, ObjectStore, ServiceConfig, ServiceConfigError,
    SharedCompactionFrontier, SharedLogDeleteRequests, Url, parse_url_opts,
    querier_object_store_prefix, shared_compaction_frontier_from_object_store,
};

pub(crate) fn compactor_delete_requests_for_config(
    config: &ServiceConfig,
    provided: Option<SharedLogDeleteRequests>,
) -> Result<SharedLogDeleteRequests, LogDeleteRequestStoreError> {
    match provided {
        Some(delete_requests) => Ok(delete_requests),
        None => SharedLogDeleteRequests::from_data_root(&config.data_root),
    }
}

#[cfg_attr(test, mutants::skip)]

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
            let store_config = parse_url_opts(&url, std::env::vars())?;
            Ok(ConfiguredObjectStore {
                store: MeteredObjectStore::wrap(Arc::from(store_config.0), metrics),
                prefix: store_config.1,
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

pub(crate) struct QuerierCompactionFrontier {
    pub(crate) frontier: Option<SharedCompactionFrontier>,
    pub(crate) refresh_source: Option<CompactionFrontierRefreshSource>,
}

pub(crate) async fn load_querier_shared_compaction_frontier(
    config: &ServiceConfig,
    configured_store: Option<&ConfiguredObjectStore>,
    object_store: Option<&dyn ObjectStore>,
) -> Result<QuerierCompactionFrontier, ServiceConfigError> {
    if let Some(configured_store) = configured_store
        && let Some(prefix) = querier_object_store_prefix(config, Some(&configured_store.prefix))?
    {
        let frontier =
            shared_compaction_frontier_from_object_store(configured_store.store.as_ref(), &prefix)
                .await?;
        return Ok(QuerierCompactionFrontier {
            frontier: Some(frontier),
            refresh_source: Some(CompactionFrontierRefreshSource {
                store: configured_store.store.clone(),
                prefix,
            }),
        });
    }

    if let Some(store) = object_store
        && let Some(prefix) = querier_object_store_prefix(config, None)?
    {
        return Ok(QuerierCompactionFrontier {
            frontier: Some(shared_compaction_frontier_from_object_store(store, &prefix).await?),
            refresh_source: None,
        });
    }

    Ok(QuerierCompactionFrontier {
        frontier: None,
        refresh_source: None,
    })
}
