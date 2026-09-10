use super::{
    Arc, BlockMetadataCache, ByteSize, CachedBlock, ObjectStore, ObjectStoreReader,
    ParquetMetaData, ParquetRecordBatchStreamBuilder, Path, Result, head_within_cap, instrument,
    unreadable,
};

/// Reads one block's Parquet footer, through `cache` when one is supplied.
///
/// This is the cheapest thing that proves a block is readable: it `head`s the
/// object and decodes the footer, and never touches a column chunk. A scan
/// uses it to find the blocks it cannot read *before* it hands a file list to
/// `DataFusion`, where one bad file fails the whole plan.
///
/// # Errors
/// Returns [`BlockStoreError::BlockUnreadable`](crate::BlockStoreError::BlockUnreadable)
/// when the object is missing or is not a decodable Parquet block, or when the
/// store fails; the error carries the backend error whole, so the caller can
/// tell those apart. Returns
/// [`BlockStoreError::InvalidBlock`](crate::BlockStoreError::InvalidBlock)
/// when the block is larger than `max_bytes`.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty, cached = tracing::field::Empty),
    err
)]
pub(crate) async fn block_metadata(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Arc<ParquetMetaData>> {
    let path = Path::from(object_key);
    let meta = head_within_cap(store, &path, object_key, max_bytes).await?;
    if let Some(cache) = cache
        && let Some(metadata) = cache.get(&meta)
    {
        tracing::Span::current().record("cached", true);
        return Ok(metadata);
    }
    tracing::Span::current().record("cached", false);

    let reader = match cache {
        Some(cache) => ObjectStoreReader::with_cache(
            Arc::clone(store),
            CachedBlock {
                meta,
                cache: cache.clone(),
            },
        ),
        None => ObjectStoreReader::new(Arc::clone(store), path),
    };
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    Ok(Arc::clone(builder.metadata()))
}
