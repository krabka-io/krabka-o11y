use super::*;

#[instrument(skip_all, fields(key = %key, len = bytes.len()), err)]
pub async fn put_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    bytes: Vec<u8>,
    retain: IndexSnapshotRetain,
) -> Result<String> {
    let snapshot_key = next_snapshot_key(key)?;
    store
        .put(&Path::from(snapshot_key.clone()), PutPayload::from(bytes))
        .await?;
    prune_old_index_snapshots(store, key, retain.into_value()).await?;
    Ok(snapshot_key)
}
