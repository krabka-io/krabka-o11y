use super::*;

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shards_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let mut shard_ranges = list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
        store,
        prefix,
        tenant,
        query_range,
    )
    .await?;
    if shard_ranges.is_empty() {
        shard_ranges =
            match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await
            {
                Ok(shard_ranges) => shard_ranges,
                Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                    Vec::new()
                }
                Err(error) => return Err(error),
            };
    }
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for shard_range in shard_ranges
        .into_iter()
        .filter(|shard_range| shard_range.overlaps(query_range))
    {
        let (label_index, block_index) =
            read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await?;

        for (series_tenant, series) in label_index.series {
            for (_, labels) in series {
                merged_labels.insert_series(series_tenant.clone(), labels);
            }
        }
        for block in block_index.blocks {
            merged_blocks.entry(block.key.object_key()).or_insert(block);
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    Ok((merged_labels, block_index))
}
