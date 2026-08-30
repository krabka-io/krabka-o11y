use super::{
    Arc, BufferedLogHotTail, CancellationToken, ObjectPath, ObjectStore, SharedCompactionFrontier,
    Time, TimeExt, refresh_compaction_frontier_and_prune, sleep,
};

#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_compaction_frontier_refresher(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    frontier: SharedCompactionFrontier,
    hot_tail: BufferedLogHotTail,
    token: CancellationToken,
    refresh_interval: Time,
) {
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
    });
}
