use super::*;

pub(crate) async fn shared_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<SharedCompactionFrontier, CompactionFrontierStoreError> {
    let frontier = SharedCompactionFrontier::default();
    load_existing_compaction_frontier(store, prefix, &frontier).await?;
    Ok(frontier)
}
