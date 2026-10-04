use super::{
    Arc, ArrowReaderMetadata, ArrowReaderOptions, BlockMetadataCache, ByteSize, ByteSizeExt,
    FOOTER_PREFETCH, ObjectStore, ObjectStoreReader, PageIndexPolicy, ParquetMetaDataReader, Path,
    ProbedBlock, Result, head_within_cap, instrument, unreadable,
};

/// `head`s one block and reads its Parquet footer, through `cache` when one is
/// supplied.
///
/// This is the cheapest thing that proves a block is readable: it `head`s the
/// object and decodes the footer, and never touches a column chunk. A scan
/// uses it to find the blocks it cannot read *before* it hands a file list to
/// `DataFusion`, where one bad file fails the whole plan.
///
/// The `head` runs on a cache hit too. It is what finds a block that deletion
/// removed after the index named it, and it is what validates a cached footer
/// against a block that was rewritten in place. See [`BlockMetadataCache`].
///
/// On a cache miss the footer read is one bounded range of
/// [`FOOTER_PREFETCH`] bytes at the end of the object. With a cache the read
/// includes the page index, so the cache holds everything that the
/// `DataFusion` Parquet reader asks for, and a scan of the block reads only
/// column chunks.
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
pub(crate) async fn probe_block(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<ProbedBlock> {
    let path = Path::from(object_key);
    let meta = head_within_cap(store, &path, object_key, max_bytes).await?;
    if let Some(cache) = cache
        && let Some(metadata) = cache.get(&meta)
    {
        crate::validate_persisted_block_format(&metadata)
            .map_err(crate::BlockStoreError::InvalidBlock)?;
        tracing::Span::current().record("cached", true);
        return Ok(ProbedBlock {
            object_key: object_key.to_string(),
            meta,
            metadata,
        });
    }
    tracing::Span::current().record("cached", false);

    // Only a cached footer reaches the `DataFusion` scan, which reads the page
    // index. A caller without a cache reads the footer for its own use.
    let page_index = if cache.is_some() {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let mut reader = ObjectStoreReader::new(Arc::clone(store), path);
    let metadata = ParquetMetaDataReader::new()
        .with_prefetch_hint(Some(FOOTER_PREFETCH.bytes_usize()))
        .with_page_index_policy(page_index)
        .load_and_finish(&mut reader, meta.size)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    let metadata = Arc::new(metadata);
    // The check that a Parquet reader makes before it reads a row: the
    // Parquet schema must convert to an Arrow schema. A block that fails it is
    // corrupt here, where a scan can still skip it, and not in `DataFusion`.
    ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())
        .map_err(|error| unreadable(object_key, error))?;
    crate::validate_persisted_block_format(&metadata)
        .map_err(crate::BlockStoreError::InvalidBlock)?;
    if let Some(cache) = cache {
        cache.put(&meta, Arc::clone(&metadata));
    }
    Ok(ProbedBlock {
        object_key: object_key.to_string(),
        meta,
        metadata,
    })
}
