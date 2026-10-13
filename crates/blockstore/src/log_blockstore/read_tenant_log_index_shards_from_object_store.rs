use super::*;

/// Merges every index shard of `tenant` that overlaps `query_range`.
///
/// An absent shard manifest is an empty shard, and so is an absent shard
/// catalog. Retention deletes the manifest of a shard it empties, and the
/// listing or the catalog this read started from can be older than that
/// deletion. A query that races the sweep would otherwise fail on a shard that
/// holds nothing.
///
/// A manifest that is present must still be well-formed. Malformed JSON and a
/// `format_version` this build does not know are errors, so a corrupt index is
/// never read as an empty one.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shards_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<LogIndexes, BlockStoreError> {
    let mut shard_ranges = list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
        store,
        prefix,
        tenant,
        query_range,
    )
    .await?;
    if shard_ranges.is_empty() {
        shard_ranges =
            read_tenant_log_index_shard_ranges_or_empty_from_object_store(store, prefix, tenant)
                .await?;
    }
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for shard_range in shard_ranges
        .into_iter()
        .filter(|shard_range| shard_range.overlaps(query_range))
    {
        let indexes =
            match read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await
            {
                Ok(indexes) => indexes,
                Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => continue,
                Err(error) => return Err(error),
            };

        for series_tenant in indexes.labels.series.keys() {
            let series = &indexes.labels.series[series_tenant];
            for fingerprint in series.keys() {
                merged_labels.insert_series(series_tenant.clone(), series[fingerprint].clone());
            }
        }
        for block in indexes.blocks.blocks {
            merged_blocks.entry(block.key.object_key()).or_insert(block);
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    Ok(LogIndexes {
        labels: merged_labels,
        blocks: block_index,
    })
}
