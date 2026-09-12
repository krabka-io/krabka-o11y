use super::{
    BTreeMap, BTreeSet, BlockDeletion, BlockDescriptor, BlockStoreError, BlockTimestampUnit,
    CompactorRunError, ObjectPath, ObjectStore, RetentionWindows, TimeRange,
    delete_tenant_log_index_shard_from_object_store, log_block_deletion, log_retention_candidates,
    plan_expired_blocks, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store, retire_blocks_from_index,
    tenant_log_index_shard_ranges, write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shard_to_object_store,
};

/// Drops one tenant's expired blocks from every index that names them, and
/// names the objects the caller has to delete.
///
/// The index is rewritten here and the objects are deleted by the caller,
/// after every index of the tenant is written. A reader that lists between the
/// two steps sees an index that does not name the block, so it never asks for
/// the object.
///
/// A shard that keeps no block leaves the catalog, and its manifest object is
/// deleted. The catalog is rewritten first, so the two steps keep the same
/// order as the rest of the sweep: index before object.
/// [`read_tenant_log_index_shards_from_object_store`] reads an absent shard
/// manifest as an empty shard, so a query that read the catalog before the
/// rewrite still succeeds.
///
/// [`read_tenant_log_index_shards_from_object_store`]: krabka_blockstore::read_tenant_log_index_shards_from_object_store
///
/// # Errors
/// Returns the block-store error when an index cannot be read or written.
/// An index a tenant has never had is not a fault.
pub(crate) async fn sweep_expired_tenant_log_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    now_ns: i64,
    windows: &dyn RetentionWindows,
) -> Result<Vec<BlockDeletion>, CompactorRunError> {
    let manifest =
        match read_tenant_log_index_manifest_from_object_store(store, prefix, tenant).await {
            Ok(indexes) => Some(indexes),
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => None,
            Err(error) => return Err(error.into()),
        };
    let mut shards = Vec::new();
    for shard_range in tenant_log_index_shard_ranges(store, prefix, tenant).await? {
        let indexes =
            read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await?;
        shards.push((shard_range, indexes));
    }

    // One descriptor per object key, so a block that both the tenant manifest
    // and a shard manifest names is offered to the expiry rule once.
    let mut blocks: BTreeMap<String, &BlockDescriptor> = BTreeMap::new();
    let shard_blocks = shards
        .iter()
        .flat_map(|(_, (_, block_index))| block_index.blocks());
    for block in manifest
        .iter()
        .flat_map(|(_, block_index)| block_index.blocks())
        .chain(shard_blocks)
    {
        blocks.entry(block.key.object_key()).or_insert(block);
    }
    let candidates = log_retention_candidates(blocks.values().copied());
    let expired: BTreeSet<String> =
        plan_expired_blocks(&candidates, now_ns, BlockTimestampUnit::Nanos, windows)
            .into_iter()
            .map(|block| block.object_key)
            .collect();
    if expired.is_empty() {
        return Ok(Vec::new());
    }

    if let Some((label_index, block_index)) = &manifest
        && let Some((next_label_index, next_block_index)) =
            retire_blocks_from_index(tenant, label_index, block_index, &expired)?
    {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            &next_label_index,
            &next_block_index,
        )
        .await?;
    }

    let mut emptied: Vec<TimeRange> = Vec::new();
    for (shard_range, (label_index, block_index)) in &shards {
        let Some((next_label_index, next_block_index)) =
            retire_blocks_from_index(tenant, label_index, block_index, &expired)?
        else {
            continue;
        };
        if next_block_index.blocks().is_empty() {
            emptied.push(*shard_range);
            continue;
        }
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            *shard_range,
            &next_label_index,
            &next_block_index,
        )
        .await?;
    }
    if !emptied.is_empty() {
        let kept: Vec<TimeRange> = shards
            .iter()
            .map(|(shard_range, _)| *shard_range)
            .filter(|shard_range| !emptied.contains(shard_range))
            .collect();
        write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, &kept).await?;
        for shard_range in &emptied {
            delete_tenant_log_index_shard_from_object_store(store, prefix, tenant, *shard_range)
                .await?;
        }
    }

    Ok(expired
        .iter()
        .map(|object_key| log_block_deletion(prefix, object_key))
        .collect())
}
