use super::{
    BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockStoreError, LabelIndex, LogIndexManifest,
    ObjectPath, ObjectStore, TimeRange, instrument, log_snapshot_error,
    log_tenant_index_shard_manifest_object_path, read_log_index_shard_snapshot_base,
};
use crate::index_snapshot::{
    DEFAULT_INDEX_SNAPSHOT_RETAIN, list_index_snapshot_objects, prune_old_index_snapshots,
    snapshot_key_for_generation,
};

#[instrument(
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// Publishes replacement indexes as a new immutable shard generation.
///
/// # Errors
/// Returns an error for invalid stored metadata, failed I/O, generation
/// exhaustion, or 64 consecutive publication conflicts.
pub async fn write_tenant_log_index_shard_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    publish_log_index_shard_snapshot(
        store,
        prefix,
        tenant,
        shard_range,
        None,
        label_index,
        block_index,
    )
    .await
}

/// Applies a captured block-index change without overwriting concurrent publications.
///
/// Blocks absent from `previous` are additions. An addition already present
/// in the first loaded manifest is not inserted again after a conflict.
/// This does not detect a replay whose first read follows a completed removal.
/// Changed or removed blocks must
/// still match their captured descriptors or their already-applied result;
/// unchanged blocks never overwrite a
/// newer descriptor. An empty result remains a manifest so a concurrent append
/// cannot race an unconditional deletion of the shard key.
///
/// # Errors
/// Returns an error for invalid manifests, failed I/O, changed captured
/// descriptors, or 64 consecutive conditional-write conflicts.
pub async fn update_tenant_log_index_shard_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
    previous: &BlockIndex,
    label_index: &LabelIndex,
    next: &BlockIndex,
) -> Result<(), BlockStoreError> {
    publish_log_index_shard_snapshot(
        store,
        prefix,
        tenant,
        shard_range,
        Some(previous),
        label_index,
        next,
    )
    .await
}

async fn publish_log_index_shard_snapshot(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
    previous: Option<&BlockIndex>,
    label_index: &LabelIndex,
    next: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let key = log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range);
    let in_shard = |block: &&BlockDescriptor| {
        block.key.tenant == tenant && block.key.time_range.overlaps(shard_range)
    };
    let previous_blocks: BTreeMap<_, _> = previous
        .into_iter()
        .flat_map(BlockIndex::blocks)
        .filter(in_shard)
        .map(|block| (block.key.object_key(), block))
        .collect();
    let next: BTreeMap<_, _> = next
        .blocks()
        .iter()
        .filter(in_shard)
        .map(|block| (block.key.object_key(), block))
        .collect();
    let mut observed_additions = BTreeSet::new();
    for attempt in 0..64 {
        let base = read_log_index_shard_snapshot_base(store, &key, tenant).await?;
        let generation = match &base {
            Some((generation, _, _)) => {
                generation
                    .checked_add(1)
                    .ok_or_else(|| object_store::Error::Generic {
                        store: "log index shard",
                        source: "manifest generation exhausted".into(),
                    })?
            }
            None => 0,
        };
        let (_, mut labels, mut blocks) = base.unwrap_or_default();
        if previous.is_some() {
            if attempt == 0 {
                observed_additions = blocks
                    .blocks()
                    .iter()
                    .map(|block| block.key.object_key())
                    .filter(|key| next.contains_key(key) && !previous_blocks.contains_key(key))
                    .collect();
            }
            apply_block_index_delta(
                &mut blocks,
                &previous_blocks,
                &next,
                &observed_additions,
                &key,
            )?;
        } else {
            labels = LabelIndex::default();
            blocks = BlockIndex::default();
            for block in next.values() {
                blocks.insert((*block).clone());
            }
        }
        let fingerprints: BTreeSet<_> = blocks
            .blocks()
            .iter()
            .flat_map(|block| block.fingerprints.iter().copied())
            .collect();
        for fingerprint in fingerprints {
            if let Some(series) = label_index.labels_for(tenant, fingerprint) {
                labels.insert_series(tenant, series.clone());
            }
        }
        let manifest =
            LogIndexManifest::from_indexes_for_tenant_shard(tenant, shard_range, &labels, &blocks);
        let path = ObjectPath::from(snapshot_key_for_generation(key.as_ref(), generation));
        match store
            .put_opts(
                &path,
                serde_json::to_vec_pretty(&manifest)?.into(),
                object_store::PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => {
                // A paused writer can recreate an old generation after pruning
                // removed that key. It has not published unless it is newest.
                let latest = list_index_snapshot_objects(store, key.as_ref())
                    .await
                    .map_err(log_snapshot_error)?
                    .pop();
                if latest.is_some_and(|meta| meta.location == path) {
                    prune_old_index_snapshots(store, key.as_ref(), DEFAULT_INDEX_SNAPSHOT_RETAIN)
                        .await
                        .map_err(log_snapshot_error)?;
                    return Ok(());
                }
            }
            Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        tokio::task::yield_now().await;
    }
    Err(object_store::Error::Generic {
        store: "log index shard",
        source: format!("manifest `{key}` lost 64 conditional writes").into(),
    }
    .into())
}

fn apply_block_index_delta(
    blocks: &mut BlockIndex,
    previous: &BTreeMap<String, &BlockDescriptor>,
    next: &BTreeMap<String, &BlockDescriptor>,
    observed_additions: &BTreeSet<String>,
    path: &ObjectPath,
) -> Result<(), BlockStoreError> {
    for block in &blocks.blocks {
        let key = block.key.object_key();
        if let Some(old) = previous.get(&key)
            && next.get(&key) != Some(old)
            && block != *old
            && next.get(&key).is_none_or(|new| block != *new)
        {
            return Err(object_store::Error::Precondition {
                path: path.to_string(),
                source: format!("log block `{key}` changed during index rewrite").into(),
            }
            .into());
        }
    }
    blocks.blocks.retain_mut(|block| {
        let key = block.key.object_key();
        if previous.contains_key(&key) {
            let Some(new) = next.get(&key) else {
                return false;
            };
            if next.get(&key) != previous.get(&key) {
                *block = (*new).clone();
            }
        }
        true
    });
    for (key, block) in next {
        if !previous.contains_key(key)
            && !observed_additions.contains(key)
            && !blocks
                .blocks
                .iter()
                .any(|current| current.key.object_key() == *key)
        {
            blocks.insert((*block).clone());
        }
    }
    Ok(())
}
