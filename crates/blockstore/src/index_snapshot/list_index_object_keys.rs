use super::*;

/// Lists every object owned by the index stored at `key`.
///
/// # Errors
///
/// Returns an object-store error when the index metadata cannot be inspected.
pub async fn list_index_object_keys(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> object_store::Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    match store.head(&Path::from(key)).await {
        Ok(meta) => {
            keys.insert(meta.location.to_string());
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    for prefix in [
        index_snapshot_prefix_for_key(key),
        index_shards_prefix_for_key(key),
        shard_payload_prefix_for_key(key),
    ] {
        let mut listing = store.list(Some(&Path::from(prefix)));
        while let Some(meta) = listing.next().await {
            keys.insert(meta?.location.to_string());
        }
    }
    Ok(keys)
}
