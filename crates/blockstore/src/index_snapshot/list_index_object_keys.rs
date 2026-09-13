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
    let snapshot_prefix = index_snapshot_prefix_for_key(key);
    let mut listing = store.list(Some(&Path::from(snapshot_prefix.as_str())));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        if snapshot_generation_from_path(&meta.location).is_ok() {
            keys.insert(meta.location.to_string());
        }
    }

    let shards_prefix = index_shards_prefix_for_key(key);
    let mut listing = store.list(Some(&Path::from(shards_prefix.as_str())));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        if parse_index_shard_location(&shards_prefix, meta.location.as_ref())
            .is_some_and(|(_, object)| object != IndexShardObject::Foreign)
        {
            keys.insert(meta.location.to_string());
        }
    }

    let payload_prefix = shard_payload_prefix_for_key(key);
    let mut listing = store.list(Some(&Path::from(payload_prefix)));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        if is_shard_payload_location(key, meta.location.as_ref()) {
            keys.insert(meta.location.to_string());
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[tokio::test]
    async fn ignores_block_subtrees_that_share_index_prefix_names() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let key = "traces.json";
        let valid = [
            key.to_string(),
            "traces/snapshots/1.json".to_string(),
            crate::index::index_unbound_series_object_key(key, "tenant-a"),
            shard_payload_object_key(key, "tenant-a", IndexShardRange::new(0, 1), "hash"),
        ];
        let foreign = [
            "traces/snapshots/tenant-a/block.parquet",
            "traces/shards/tenant-a/block.parquet",
            "traces/payloads/tenant-a/block.parquet",
        ];
        for object in valid.iter().map(String::as_str).chain(foreign) {
            store
                .put(&Path::from(object), PutPayload::from_static(b"object"))
                .await
                .expect("write object");
        }

        check!(
            list_index_object_keys(&store, key)
                .await
                .expect("list keys")
                == valid.into()
        );
    }
}
