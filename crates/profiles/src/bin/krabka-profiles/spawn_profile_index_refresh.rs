use super::*;

/// How long each hot WAL-tail poll waits for records.
/// Periodically reload the profile block index from object storage and swap it
/// into the cold store. The block-builder writes new blocks continuously.
/// Without this reload the querier only ever sees the index snapshot that it
/// loaded at boot, so blocks created after boot stay invisible. The symptom is
/// that recent profiles return empty, above all sparse ones such as memory that
/// age out of the hot tier. This loop mirrors the `TraceIndex` refresh loop of
/// the traces querier.
pub(crate) fn spawn_profile_index_refresh(
    cold: Arc<ColdProfileStore>,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    max_bytes: ByteSize,
    interval: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval.to_std());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = tick.tick() => {}
            }
            match ProfileIndex::load_latest_snapshot_with_max_bytes(&store, &index_key, max_bytes)
                .await
            {
                Ok(index) => cold.replace_index(Arc::new(index)),
                Err(error) => {
                    tracing::warn!(%error, %index_key, "profile index refresh failed; retaining last good index");
                }
            }
        }
    });
}
