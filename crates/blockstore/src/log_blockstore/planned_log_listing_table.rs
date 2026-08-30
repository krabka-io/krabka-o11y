use super::*;

pub(crate) fn planned_log_listing_table(
    root: impl AsRef<Path>,
    blocks: &[BlockDescriptor],
    schema: Arc<Schema>,
) -> Result<ListingTable, BlockStoreError> {
    validate_planned_blocks(blocks)?;

    let table_paths = blocks
        .iter()
        .map(|block| {
            let path = block_path(root.as_ref(), &block.key);
            ListingTableUrl::parse(
                path.to_str()
                    .ok_or(BlockStoreError::NonUtf8BlockPath { path: path.clone() })?,
            )
            .map_err(BlockStoreError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let config = ListingTableConfig::new_with_multi_paths(table_paths)
        .with_listing_options(listing_options)
        .with_schema(schema);
    Ok(ListingTable::try_new(config)?)
}
