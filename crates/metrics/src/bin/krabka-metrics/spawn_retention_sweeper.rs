use krabka_observability::CancellationToken;

use super::{Arc, ObjectStore, Time, TimeExt, unix_time_ms};

/// Runs the compaction-retention sweep on a timer, returning its handle for
/// the caller to supervise.
///
/// A sweep that merely fails is not an exit: the loop logs it and tries again
/// on the next interval. An exit means the sweeper is gone, and a compactor
/// that has stopped enforcing retention says nothing about it -- the object
/// store simply keeps growing until something else notices.
// cargo-mutants: background wall-clock loop is exercised through compactor integration.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_retention_sweeper(
    store: Arc<dyn ObjectStore>,
    retention: Time,
    sweep_interval: Time,
    stopping: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match krabka_metrics::enforce_compaction_retention(
                store.clone(),
                unix_time_ms(),
                retention,
            )
            .await
            {
                Ok(stats) => {
                    if stats.manifests_deleted > 0 || stats.blocks_deleted > 0 {
                        tracing::info!(
                            manifests_scanned = stats.manifests_scanned,
                            manifests_deleted = stats.manifests_deleted,
                            blocks_deleted = stats.blocks_deleted,
                            "metrics compactor retention deleted old blocks"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "metrics compactor retention sweep failed");
                }
            }
            tokio::select! {
                () = stopping.cancelled() => break,
                () = tokio::time::sleep(sweep_interval.to_std()) => {}
            }
        }
    })
}
