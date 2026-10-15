use super::*;

use crate::index_snapshot::{
    DEFAULT_INDEX_SNAPSHOT_RETAIN, list_index_snapshot_objects, prune_old_index_snapshots,
    snapshot_key_for_generation,
};

#[instrument(
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition, rows = rows.len()),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn write_log_block(
    root: impl AsRef<Path>,
    key: &BlockKey,
    mut rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    validate_rows(key, &rows)?;
    sort_log_rows(&mut rows);

    let path = block_path(root, key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let schema = log_block_schema();
    let batch = rows_to_batch(&rows, Arc::clone(&schema))?;
    let mut writer = ArrowWriter::try_new(
        File::create(&path)?,
        schema,
        Some(crate::persisted_format::persisted_block_writer_properties()),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    let size = ByteSize::from_bytes(fs::metadata(&path)?.len());

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        size,
    ))
}

#[instrument(
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition, rows = rows.len(), size = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    mut rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    validate_rows(key, &rows)?;
    sort_log_rows(&mut rows);

    let payload = encode_log_block(&rows)?;
    let size_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    tracing::Span::current().record("size", size_bytes);
    store
        .put(&log_block_object_path(prefix, key), payload.into())
        .await?;

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        ByteSize::from_bytes(size_bytes),
    ))
}

#[instrument(skip_all, fields(tenant = %tenant, shards = shard_ranges.len()), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shard_catalog_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_ranges: &[TimeRange],
) -> Result<(), BlockStoreError> {
    let catalog = LogIndexShardCatalog::new(shard_ranges);
    let payload = serde_json::to_vec_pretty(&catalog)?;
    store
        .put(
            &log_tenant_index_shard_catalog_object_path(prefix, tenant),
            payload.into(),
        )
        .await?;
    Ok(())
}

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
    let mut previous_blocks = BTreeMap::new();
    for block in previous
        .into_iter()
        .flat_map(BlockIndex::blocks)
        .filter(in_shard)
    {
        previous_blocks.insert(block.key.object_key(), block);
    }
    let mut next_blocks = BTreeMap::new();
    for block in next.blocks().iter().filter(in_shard) {
        next_blocks.insert(block.key.object_key(), block);
    }
    let mut observed_additions = BTreeSet::new();
    for attempt in 0..64 {
        let base = read_log_index_shard_snapshot_base(store, &key, tenant).await?;
        let generation =
            match &base {
                Some(snapshot) => snapshot.generation.checked_add(1).ok_or_else(|| {
                    object_store::Error::Generic {
                        store: "log index shard",
                        source: "manifest generation exhausted".into(),
                    }
                })?,
                None => 0,
            };
        let snapshot = base.unwrap_or_default();
        let mut labels = snapshot.indexes.labels;
        let mut blocks = snapshot.indexes.blocks;
        if previous.is_some() {
            if attempt == 0 {
                observed_additions = blocks
                    .blocks()
                    .iter()
                    .map(|block| block.key.object_key())
                    .filter(|key| {
                        next_blocks.contains_key(key) && !previous_blocks.contains_key(key)
                    })
                    .collect();
            }
            apply_block_index_delta(
                &mut blocks,
                &previous_blocks,
                &next_blocks,
                &observed_additions,
                &key,
            )?;
        } else {
            labels = LabelIndex::default();
            blocks = BlockIndex::default();
            for block in next_blocks.values() {
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
    for key in next.keys() {
        let block = next[key];
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

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shards_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_ranges: &[TimeRange],
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    for shard_range in shard_ranges {
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            *shard_range,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, shard_ranges).await
}


/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn register_log_blocks(
    ctx: &SessionContext,
    table_name: &str,
    root: impl AsRef<Path>,
    blocks: &[BlockDescriptor],
) -> Result<(), BlockStoreError> {
    let table = Arc::new(LogBlockTableProvider::try_new(root, blocks)?);
    ctx.register_table(table_name, table)?;
    Ok(())
}


/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn register_log_blocks_from_object_store(
    ctx: &SessionContext,
    table_name: &str,
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    blocks: &[BlockDescriptor],
) -> Result<(), BlockStoreError> {
    let table = Arc::new(LogBlockTableProvider::try_new_object_store(
        store, prefix, blocks,
    )?);
    ctx.register_table(table_name, table)?;
    Ok(())
}

/// Deletes the retained generations of one index shard.
///
/// Publishers for the shard should be stopped before calling this function.
///
/// # Errors
/// Returns an error when listing or deletion fails, except for absent objects.
#[instrument(skip_all, err)]
pub async fn delete_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<(), BlockStoreError> {
    let key = log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range);
    for meta in list_index_snapshot_objects(store, key.as_ref())
        .await
        .map_err(log_snapshot_error)?
    {
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
