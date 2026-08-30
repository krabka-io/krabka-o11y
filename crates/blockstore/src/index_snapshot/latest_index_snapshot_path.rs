use super::*;

pub async fn latest_index_snapshot_path(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<Path>> {
    Ok(list_index_snapshot_objects(store, key)
        .await?
        .pop()
        .map(|meta| meta.location))
}
