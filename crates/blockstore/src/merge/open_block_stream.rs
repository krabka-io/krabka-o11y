use super::{
    Arc, BlockBatchStream, BlockObjectReader, BlockReadFailure, BlockStoreError, ByteSize,
    ByteSizeExt, ObjectMeta, ObjectStore, ObjectStoreExt, ParquetRecordBatchStreamBuilder, Path,
    Result, SchemaRef, StreamExt, TryStreamExt,
};

/// Opens the block at `object_key` as a stream of batches, and reports the
/// schema it was written with.
///
/// The schema comes back before any row does, which is what lets a caller
/// settle the merged output's schema -- the union of its inputs' promoted
/// columns, say -- from the footers alone rather than by reading the blocks.
///
/// Unlike [`read_block_with_max_bytes`](crate::read_block_with_max_bytes) this
/// leaves the block on the object store: the returned stream fetches and
/// decodes a row group at a time, so an input costs one batch of memory rather
/// than all of it. `max_bytes` is still checked against the object's on-disk
/// size, for the same reason the buffered reader checks it -- an
/// unexpectedly huge block is a sign something is wrong upstream, and it is
/// better to say so than to grind through it.
///
/// # Errors
/// Returns [`BlockStoreError::BlockUnreadable`] when the object is missing or
/// is not a Parquet block, [`BlockStoreError::InvalidBlock`] when it is larger
/// than `max_bytes`, and [`BlockStoreError::ObjectStore`] when the store
/// itself fails.
pub async fn open_block_stream(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    batch_rows: usize,
) -> Result<(ObjectMeta, SchemaRef, BlockBatchStream)> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await.map_err(|error| {
        BlockStoreError::block_unreadable(object_key, BlockReadFailure::ObjectStore(error))
    })?;
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {} bytes",
            meta.size,
            max_bytes.bytes_u64()
        )));
    }

    let reader = BlockObjectReader::new(store, meta.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| {
            BlockStoreError::block_unreadable(object_key, BlockReadFailure::Parquet(error))
        })?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(BlockStoreError::InvalidBlock)?;
    let builder = builder.with_batch_size(batch_rows);
    let key = object_key.to_string();
    let batches = builder.build().map_err(|error| {
        BlockStoreError::block_unreadable(object_key, BlockReadFailure::Parquet(error))
    })?;
    // The built stream preserves Arrow types such as dictionaries while
    // stripping file-level Parquet metadata from the logical batch schema.
    let schema = Arc::clone(batches.schema());
    let batches = batches
        .map_err(move |error| {
            BlockStoreError::block_unreadable(&key, BlockReadFailure::Parquet(error))
        })
        .boxed();
    Ok((meta, schema, batches))
}
