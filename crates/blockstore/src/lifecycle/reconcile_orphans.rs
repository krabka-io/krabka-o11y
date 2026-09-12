use futures::StreamExt as _;

use super::{
    BTreeSet, LifecycleError, ObjectStore, OrphanSweepStats, Path, SystemTime, Time, TimeExt,
    UNIX_EPOCH, instrument,
};

/// Deletes the objects under `prefix` that `live_keys` does not name.
///
/// An index is the only record of which blocks are live, so an object the
/// index does not name is unreachable and nothing will ever read it. A write
/// that failed after it put the block, a compaction whose index save lost the
/// race, and a deletion pass that was interrupted all leave one behind.
///
/// # The grace window
///
/// `grace` is what keeps the sweep from deleting a live block. **A block is
/// written before its index entry is saved**, so a writer that has put its
/// block and not yet published it is indistinguishable from one that never
/// will: both are objects no index names. Without the grace window this
/// deletes freshly-written blocks, including ones another replica is
/// publishing at that moment.
///
/// An object whose `last_modified` is newer than `now - grace` is therefore
/// left alone, and so is an object the sweep cannot date. Pass
/// [`DEFAULT_BLOCK_SWEEP_GRACE`](super::DEFAULT_BLOCK_SWEEP_GRACE) unless the
/// caller knows its own publication is faster.
///
/// # Scope
///
/// Every object under `prefix` that `live_keys` does not name is deleted, so
/// `prefix` must name a location the blocks own. A prefix shared with anything
/// else loses that other thing.
///
/// One object that will not delete does not end the pass; it is counted in
/// [`OrphanSweepStats::failed`] and the next pass sees it again. An object
/// already gone counts in [`OrphanSweepStats::absent`].
///
/// # Errors
/// Returns [`LifecycleError::ObjectStore`] when listing `prefix` fails. The
/// sweep then knows nothing about what exists, and deleting on that basis
/// would delete live blocks.
#[instrument(
    level = "debug",
    skip_all,
    fields(
        prefix = %prefix,
        listed = tracing::field::Empty,
        deleted = tracing::field::Empty,
    ),
    err
)]
pub async fn reconcile_orphans(
    store: &dyn ObjectStore,
    prefix: &str,
    live_keys: &BTreeSet<String>,
    grace: Time,
    now: SystemTime,
) -> Result<OrphanSweepStats, LifecycleError> {
    let cutoff = now
        .checked_sub(grace.to_std())
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_secs()).ok());

    let mut stats = OrphanSweepStats::default();
    let mut stale = Vec::new();
    let mut listing = store.list(Some(&Path::from(prefix)));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        stats.listed += 1;
        if live_keys.contains(meta.location.as_ref()) {
            stats.live += 1;
            continue;
        }
        // At or before the cutoff, not strictly before it: object timestamps
        // are whole seconds, and a zero grace has to mean "sweep everything"
        // rather than "sweep everything written in a previous second".
        if cutoff.is_some_and(|cutoff| meta.last_modified.timestamp() <= cutoff) {
            stale.push(meta.location);
        } else {
            stats.kept_within_grace += 1;
        }
    }

    let locations = futures::stream::iter(stale.into_iter().map(Ok)).boxed();
    let mut deletions = store.delete_stream(locations);
    while let Some(outcome) = deletions.next().await {
        match outcome {
            Ok(_) => stats.deleted += 1,
            Err(object_store::Error::NotFound { .. }) => stats.absent += 1,
            Err(error) => {
                stats.failed += 1;
                // The stream does not say which object failed, so the message
                // is all there is to report. The object stays, and the next
                // pass reaches it again.
                tracing::warn!(%error, %prefix, "could not delete an orphaned block object");
            }
        }
    }

    tracing::Span::current().record("listed", stats.listed);
    tracing::Span::current().record("deleted", stats.deleted);
    Ok(stats)
}
