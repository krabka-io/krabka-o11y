use super::{Arc, AtomicBool, ObjectStore, Ordering, Time, TimeExt, unix_time_ms};

// cargo-mutants: background wall-clock loop is exercised through compactor integration.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_retention_sweeper(
    store: Arc<dyn ObjectStore>,
    retention: Time,
    sweep_interval: Time,
    stopping: Arc<AtomicBool>,
) {
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
            if stopping.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(sweep_interval.to_std()).await;
            if stopping.load(Ordering::SeqCst) {
                break;
            }
        }
    });
}
