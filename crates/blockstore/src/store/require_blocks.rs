use super::{Arc, BlockMetadataCache, ByteSize, ObjectStore, ProbedBlock, Result, instrument};

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
    super::probe_blocks::probe_keys(store, keys, max_bytes, cache)
        .await
        .into_iter()
        .collect()
}
