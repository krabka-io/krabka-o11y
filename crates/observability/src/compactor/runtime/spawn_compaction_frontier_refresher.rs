use super::{
    Arc, BufferedLogHotTail, CancellationToken, JoinHandle, ObjectPath, ObjectStore,
    SharedCompactionFrontier, Time, TimeExt, refresh_compaction_frontier_and_prune, sleep,
};

/// Starts the querier's compaction-frontier refresher and hands its
/// [`JoinHandle`] to the caller.
///
/// The handle is the whole return value, and the caller must keep it. The task
/// is the only thing that moves the querier's frontier forward and prunes what
/// the compactor has taken over, so if it stops, the querier keeps answering
/// from the frontier it last read, and from hot-tail records already published
/// as blocks. Nothing about the answers says they are stale. A dropped handle
/// makes that outcome unobservable: a panic inside the loop ends the task and
/// leaves nobody to notice.
///
/// Push the handle onto the caller's background-task list, under a name, so
/// the service loop that supervises those tasks treats its exit the way it
/// treats the hot-tail poller's — cancel the shutdown token and fail the
/// process, rather than serve stale reads.
///
/// A refresh that merely fails is not an exit: the loop logs it, keeps the
/// last good frontier, and tries again on the next tick. Only cancellation
/// (a clean return) and a panic end the task.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_compaction_frontier_refresher(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    frontier: SharedCompactionFrontier,
    hot_tail: BufferedLogHotTail,
    token: CancellationToken,
    refresh_interval: Time,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                () = sleep(refresh_interval.to_std()) => {}
            }

            if let Err(error) =
                refresh_compaction_frontier_and_prune(store.as_ref(), &prefix, &frontier, &hot_tail)
                    .await
            {
                tracing::warn!(%error, "compaction frontier refresh failed; retaining last good frontier");
            }
        }
    })
}
