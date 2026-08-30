use super::{
    CompactionFrontierRefreshSource, ConfiguredObjectStore, ObjectStore, ServiceConfig,
    ServiceConfigError, SharedCompactionFrontier, querier_object_store_prefix,
    shared_compaction_frontier_from_object_store,
};

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
