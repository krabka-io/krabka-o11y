use super::*;

pub(crate) async fn load_existing_compaction_frontier(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    match read_compaction_frontier_from_object_store(store, prefix).await {
        Ok(loaded) => frontier.replace(loaded),
        Err(CompactionFrontierStoreError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}
