use super::{
    Arc, ObjectStore, OrphanSweepStats, ProfileIndex, ProfilesError, SystemTime, Time,
    live_object_keys, reconcile_orphans,
};

/// Deletes the objects under `prefix` that the index does not name.
///
/// A write that failed after it put the block, and a compaction whose index
/// save never landed, both leave an object nothing will ever read.
///
/// `grace` is what keeps the sweep off a block that is about to be live, or
/// that already is. Two cases need it: a block-builder that has put its block
/// and not yet published the index entry, and a block the builder published
/// after this pass loaded `index`. Neither appears in the live set, and both
/// are newer than the grace window. Pass
/// [`DEFAULT_BLOCK_SWEEP_GRACE`](krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE).
///
/// The live set is [`live_object_keys`], which names each live block **and its
/// symbol database**. Read that function before you change what is passed
/// here.
///
/// An index that names no block at all stops the sweep, and it reports nothing
/// listed. An empty index is what a first start sees, and it is also what a
/// deployment whose index snapshot failed to publish sees. The two are
/// indistinguishable from here, and one of them is a bucket the sweep would
/// empty.
///
/// # Errors
/// Returns [`ProfilesError::Block`] when the prefix cannot be listed. The
/// sweep then knows nothing about what exists, and deleting on that basis
/// would delete live blocks.
pub async fn sweep_orphan_blocks(
    store: &Arc<dyn ObjectStore>,
    index: &ProfileIndex,
    prefix: &str,
    grace: Time,
    now: SystemTime,
) -> Result<OrphanSweepStats, ProfilesError> {
    let live = live_object_keys(index);
    if live.is_empty() {
        tracing::debug!(%prefix, "profiles orphan sweep skipped: the index names no block");
        return Ok(OrphanSweepStats::default());
    }
    reconcile_orphans(store, prefix, &live, grace, now)
        .await
        .map_err(|error| ProfilesError::Block(error.to_string()))
}
