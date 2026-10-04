use super::{Arc, BlockMetadataCache, ByteSize, ObjectStore, ParquetMetaData, Result, probe_block};

/// Reads one block's Parquet footer, through `cache` when one is supplied.
///
/// See [`probe_block`] for what the read costs and why.
///
/// # Errors
/// See [`probe_block`].
pub(crate) async fn block_metadata(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Arc<ParquetMetaData>> {
    probe_block(store, object_key, max_bytes, cache)
        .await
        .map(|probed| probed.metadata)
}
