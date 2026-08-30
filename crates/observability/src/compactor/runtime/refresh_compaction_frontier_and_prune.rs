use super::*;

pub(crate) async fn refresh_compaction_frontier_and_prune(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
    hot_tail: &BufferedLogHotTail,
) -> Result<usize, CompactionFrontierStoreError> {
    let updated = match read_compaction_frontier_from_object_store(store, prefix).await {
        Ok(updated) => updated,
        Err(CompactionFrontierStoreError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => return Ok(0),
        Err(error) => return Err(error),
    };
    frontier.replace(updated.clone());
    Ok(hot_tail.prune_compacted(&updated))
}
