use krabka_blockstore::LogIndexes;

use super::{
    BTreeMap, BTreeSet, BlockDeletion, BlockDescriptor, BlockStoreError, BlockTimestampUnit,
    CompactorRunError, ObjectPath, ObjectStore, RetentionWindows, log_block_deletion,
    log_retention_candidates, plan_expired_blocks,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store, retire_blocks_from_index,
    tenant_log_index_shard_ranges, update_tenant_log_index_shard_to_object_store,
    write_tenant_log_index_manifest_to_object_store,
};

struct ShardLogIndexes {
    range: super::TimeRange,
    indexes: LogIndexes,
}

/// Drops one tenant's expired blocks from every index that names them, and
/// names the objects the caller has to delete.
///
/// The index is rewritten here and the objects are deleted by the caller,
/// after every index of the tenant is written. A reader that lists between the
/// two steps sees an index that does not name the block, so it never asks for
/// the object.
///
/// Empty shards retain an empty manifest. Deleting their generation keys can
/// remove a concurrent append. Pruning keeps the newest eight generations per
/// range; empty ranges continue to accumulate.
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
        shards.push(ShardLogIndexes {
            range: shard_range,
            indexes,
        });
    }

    // One descriptor per object key, so a block that both the tenant manifest
    // and a shard manifest names is offered to the expiry rule once.
    let mut blocks: BTreeMap<String, &BlockDescriptor> = BTreeMap::new();
    let shard_blocks = shards
        .iter()
        .flat_map(|shard| shard.indexes.blocks.blocks());
    for block in manifest
        .iter()
        .flat_map(|indexes| indexes.blocks.blocks())
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

    if let Some(indexes) = &manifest
        && let Some(next_indexes) =
            retire_blocks_from_index(tenant, &indexes.labels, &indexes.blocks, &expired)?
    {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            &next_indexes.label_index,
            &next_indexes.block_index,
        )
        .await?;
    }

    for shard in &shards {
        let shard_range = shard.range;
        let indexes = &shard.indexes;
        let Some(next_indexes) =
            retire_blocks_from_index(tenant, &indexes.labels, &indexes.blocks, &expired)?
        else {
            continue;
        };
        update_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            shard_range,
            &indexes.blocks,
            &next_indexes.label_index,
            &next_indexes.block_index,
        )
        .await?;
    }

    Ok(expired
        .iter()
        .map(|object_key| log_block_deletion(prefix, object_key))
        .collect())
}
