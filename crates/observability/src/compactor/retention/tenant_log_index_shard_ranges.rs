use super::{
    BlockStoreError, CompactorRunError, ObjectPath, ObjectStore, TimeRange,
    list_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
};

/// Every shard of `tenant` that the sweep has to rewrite.
///
/// The catalog and the shard listing can disagree, and the sweep needs the
/// union of the two. A shard manifest that the catalog does not name is still
/// found by the querier, which lists the shard prefix first and reads the
/// catalog only when that listing is empty. A sweep that read the catalog
/// alone would delete the blocks of such a shard and leave the manifest naming
/// them.
///
/// # Errors
/// Returns the block-store error for every failure except an absent catalog.
/// A tenant that has never had a catalog written is not a fault.
pub(crate) async fn tenant_log_index_shard_ranges(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, CompactorRunError> {
    let catalogued =
        match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
    let listed =
        list_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await?;

    // `TimeRange` is not `Ord`, so the duplicates go after the sort rather
    // than through a set.
    let mut shard_ranges: Vec<TimeRange> = catalogued.into_iter().chain(listed).collect();
    shard_ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    shard_ranges.dedup();
    Ok(shard_ranges)
}
