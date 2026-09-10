use super::{
    Arc, BLOCK_PROBE_CONCURRENCY, BlockMetadataCache, ByteSize, ObjectStore, Result, SkippedBlock,
    StreamExt, TryStreamExt, block_metadata, instrument, stream,
};

/// Splits `keys` into the blocks a scan can read and the blocks it must skip.
///
/// Reading one block's footer is what settles the question, and it is cheap: a
/// `head` and a tail read, the second of which the footer cache serves on a
/// repeat query. Doing it here, before the keys reach `DataFusion`, is the
/// point — `DataFusion` fails a plan if any one file in it fails to decode,
/// and silently omits any path that lists to nothing, so left to itself it
/// turns one bad block into either a failed query or an under-reported one.
///
/// A failure that is not the block's own — the store unreachable, a rejected
/// credential — is returned, not skipped. See
/// [`BlockReadFailure::skip_reason`](crate::BlockReadFailure::skip_reason).
///
/// # Errors
/// Returns an error when a block cannot be read for a reason that is not that
/// block's fault, or when a block exceeds the store's read cap.
#[instrument(level = "debug", skip_all, fields(candidates = keys.len(), skipped = tracing::field::Empty))]
pub(crate) async fn probe_blocks(
    store: &Arc<dyn ObjectStore>,
    keys: &[String],
    max_bytes: ByteSize,
    cache: &BlockMetadataCache,
) -> Result<(Vec<String>, Vec<SkippedBlock>)> {
    // Owned captures, and no closure; see `require_blocks` for why.
    let mut probes = Vec::with_capacity(keys.len());
    for object_key in keys {
        let store = Arc::clone(store);
        let object_key = object_key.clone();
        let cache = cache.clone();
        probes.push(async move {
            match block_metadata(&store, &object_key, max_bytes, Some(&cache)).await {
                Ok(_) => Ok(Ok(object_key)),
                Err(error) => match error.skipped_block() {
                    Some(skipped) => Ok(Err(skipped)),
                    None => Err(error),
                },
            }
        });
    }

    // `buffered`, not `buffer_unordered`: the readable keys keep the order the
    // index gave them, so the scan and the report read the same way run to run.
    let outcomes = stream::iter(probes)
        .buffered(BLOCK_PROBE_CONCURRENCY)
        .try_collect::<Vec<std::result::Result<String, SkippedBlock>>>()
        .await?;

    let mut readable = Vec::with_capacity(outcomes.len());
    let mut skipped = Vec::new();
    for outcome in outcomes {
        match outcome {
            Ok(object_key) => readable.push(object_key),
            Err(block) => skipped.push(block),
        }
    }
    tracing::Span::current().record("skipped", skipped.len());
    for block in &skipped {
        tracing::warn!(
            object_key = %block.object_key,
            reason = %block.reason,
            detail = %block.detail,
            "skipping unreadable block"
        );
    }
    Ok((readable, skipped))
}
