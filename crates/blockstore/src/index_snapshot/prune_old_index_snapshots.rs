use super::*;

#[instrument(level = "debug", skip_all, fields(key = %key, retain), err)]
pub(crate) async fn prune_old_index_snapshots(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    retain: usize,
) -> Result<()> {
    let objects = list_index_snapshot_objects(store, key).await?;
    let stale = objects.len().saturating_sub(retain);
    for meta in objects.into_iter().take(stale) {
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}
