use futures::StreamExt as _;

use super::{BlockStoreError, ByteSize, ObjectPath, ObjectStore, head_log_block_within_cap};

/// How many `head` requests the cap pre-pass keeps in flight.
///
/// A planned scan is tens of blocks at most, and each request is one round
/// trip that returns a few hundred bytes. Issuing them one at a time would put
/// the object store's latency on the query's critical path once per block.
const HEAD_CONCURRENCY: usize = 16;

/// Rejects the scan if any planned block is larger than `max_bytes`.
///
/// Blocks come from shared object storage and, per the threat model, may be
/// corrupt or maliciously oversized; this is the same guard `crate::reader`
/// applies before it opens a block for every other signal. The Parquet scan
/// beneath this one is streaming, so an oversized block no longer has to be
/// resident all at once, but its footer, its page index and a single row group
/// still do, and none of those are bounded by anything the reader controls.
/// One `head` per block turns that into an error.
pub(crate) async fn head_log_blocks_within_cap(
    store: &dyn ObjectStore,
    block_paths: &[ObjectPath],
    max_bytes: ByteSize,
) -> Result<(), BlockStoreError> {
    let pending = block_paths
        .iter()
        .map(|path| head_log_block_within_cap(store, path, max_bytes))
        .collect::<Vec<_>>();
    let mut heads = futures::stream::iter(pending).buffer_unordered(HEAD_CONCURRENCY);

    while let Some(result) = heads.next().await {
        result?;
    }
    Ok(())
}
