use crate::{
    Arc, CompactionFrontierRefreshSource, ConfiguredObjectStore, LocalFileSystem,
    LogDeleteRequestStoreError, ObjectPath, ObjectStore, ServiceConfig, ServiceConfigError,
    SharedCompactionFrontier, SharedLogDeleteRequests, Url, parse_url_opts,
    querier_object_store_prefix, shared_compaction_frontier_from_object_store,
};
#[cfg_attr(test, mutants::skip)]
pub(crate) fn build_configured_object_store(
    config: &ServiceConfig,
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
                store: Arc::new(LocalFileSystem::new_with_prefix(path)?),
                prefix: ObjectPath::from(""),
            }))
        }
        Ok(url) => {
            let (store, prefix) = parse_url_opts(&url, std::env::vars())?;
            Ok(Some(ConfiguredObjectStore {
                store: Arc::from(store),
                prefix,
            }))
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => Ok(Some(ConfiguredObjectStore {
            store: Arc::new(LocalFileSystem::new_with_prefix(raw_url)?),
            prefix: ObjectPath::from(""),
        })),
        Err(error) => Err(ServiceConfigError::InvalidObjectStoreUrl {
            url: raw_url.to_string(),
            reason: error.to_string(),
        }),
    }
}

pub(crate) async fn load_querier_shared_compaction_frontier(
    config: &ServiceConfig,
    configured_store: Option<&ConfiguredObjectStore>,
    object_store: Option<&dyn ObjectStore>,
) -> Result<
    (
        Option<SharedCompactionFrontier>,
        Option<CompactionFrontierRefreshSource>,
    ),
    ServiceConfigError,
> {
    if let Some(configured_store) = configured_store
        && let Some(prefix) = querier_object_store_prefix(config, Some(&configured_store.prefix))?
    {
        let frontier =
            shared_compaction_frontier_from_object_store(configured_store.store.as_ref(), &prefix)
                .await?;
        return Ok((
            Some(frontier),
            Some((configured_store.store.clone(), prefix)),
        ));
    }

    if let Some(store) = object_store
        && let Some(prefix) = querier_object_store_prefix(config, None)?
    {
        return Ok((
            Some(shared_compaction_frontier_from_object_store(store, &prefix).await?),
            None,
        ));
    }

    Ok((None, None))
}

pub(crate) fn compactor_delete_requests_for_config(
    config: &ServiceConfig,
    provided: Option<SharedLogDeleteRequests>,
) -> Result<SharedLogDeleteRequests, LogDeleteRequestStoreError> {
    match provided {
        Some(delete_requests) => Ok(delete_requests),
        None => SharedLogDeleteRequests::from_data_root(&config.data_root),
    }
}
