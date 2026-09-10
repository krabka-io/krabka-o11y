use super::{
    Arc, BlockMetadataCache, ByteSize, ByteSizeExt, ObjectStore, Result, RowGroupMeta,
    block_metadata,
};

/// Reads row-group sizes from a block's footer, through `cache` when supplied.
///
/// # Errors
/// See [`block_metadata`].
pub(crate) async fn row_group_metadata(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Vec<RowGroupMeta>> {
    let metadata = block_metadata(store, object_key, max_bytes, cache).await?;
    Ok(metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(index, row_group)| RowGroupMeta {
            index,
            compressed: ByteSize::from_bytes(
                u64::try_from(row_group.compressed_size()).unwrap_or(0),
            ),
        })
        .collect())
}
