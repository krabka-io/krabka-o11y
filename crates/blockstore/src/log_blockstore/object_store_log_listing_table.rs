use super::{
    Arc, BlockStoreError, ListingTable, ListingTableConfig, ListingTableUrl, ObjectPath,
    ObjectStoreUrl, Schema, log_listing_options,
};

/// The `ListingTable` that reads `block_paths` out of the object store
/// registered under `object_store_url`.
///
/// One table over every planned block, so the scan `DataFusion` builds from it
/// is a single Parquet `DataSourceExec` whose file groups the optimiser is free
/// to spread across partitions -- the same plan shape
/// `BlockStore::register_scan_table` produces for every other signal.
///
/// Each path names one object rather than a prefix, so the listing is a `head`
/// per block and never a `list` over the tenant's whole block prefix.
pub(crate) fn object_store_log_listing_table(
    object_store_url: &ObjectStoreUrl,
    block_paths: &[ObjectPath],
    schema: Arc<Schema>,
) -> Result<ListingTable, BlockStoreError> {
    let table_paths = block_paths
        .iter()
        .map(|path| ListingTableUrl::parse(format!("{object_store_url}{path}")))
        .collect::<Result<Vec<_>, _>>()?;
    let config = ListingTableConfig::new_with_multi_paths(table_paths)
        .with_listing_options(log_listing_options())
        .with_schema(schema);
    Ok(ListingTable::try_new(config)?)
}
