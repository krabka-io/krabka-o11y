use super::{
    Arc, BlockDescriptor, BlockStoreError, ListingTable, ListingTableUrl, Path, Schema, block_path,
    log_listing_table_from_paths, validate_planned_blocks,
};

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
    log_listing_table_from_paths(table_paths, schema)
}
