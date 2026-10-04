use super::{
    Arc, BLOCK_PROBE_CONCURRENCY, BlockMetadataCache, ByteSize, ObjectStore, ProbedBlock, Result,
    StreamExt, TryStreamExt, instrument, probe_block, stream,
};

/// Fails unless every one of `keys` is readable.
///
/// `DataFusion` resolves each path it is given as a listing, so a key with no
/// object behind it contributes no files and no error: the scan returns the
/// other blocks' rows and says nothing. That is a wrong answer delivered
/// confidently, and it is why this runs before the keys reach `DataFusion`
/// rather than being left to it.
///
/// The footer each probe reads is cached, so on a warm store the check costs
/// one `head` per block, and one footer read per block the first time. The scan
/// reads the blocks with the `ObjectMeta` and footer this returns, so it does
/// not `head` them again.
///
/// # Errors
/// Returns [`BlockStoreError::BlockUnreadable`](crate::BlockStoreError::BlockUnreadable)
/// for the first block that cannot be read.
#[instrument(level = "debug", skip_all, fields(keys = keys.len()), err)]
pub(crate) async fn require_blocks(
    store: &Arc<dyn ObjectStore>,
    keys: &[String],
    max_bytes: ByteSize,
    cache: &BlockMetadataCache,
) -> Result<Vec<ProbedBlock>> {
    // Every probe owns its key, its store handle and its cache handle, and is
    // built here rather than in a closure. A closure that borrowed them would
    // return a future tied to those borrows' lifetimes, so it would implement
    // `FnOnce` for one lifetime rather than for any — and every `async fn` up
    // the call chain would stop being `Send`.
    let mut probes = Vec::with_capacity(keys.len());
    for object_key in keys {
        let store = Arc::clone(store);
        let object_key = object_key.clone();
        let cache = cache.clone();
        probes.push(async move { probe_block(&store, &object_key, max_bytes, Some(&cache)).await });
    }

    stream::iter(probes)
        .buffered(BLOCK_PROBE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
}
