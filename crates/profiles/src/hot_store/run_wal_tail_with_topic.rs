use super::{
    AutoOffsetReset, CancellationToken, Consumer, ProfileRecord, ProfilesError, WalTailConfig,
    WalTailProfileStore,
};

/// Consume the configured profiles WAL topic into the hot query store until
/// `shutdown` is cancelled.
///
/// The cancellation is a drain, not an abort, on the same terms as the
/// block-builder's: a poll that has already returned is decoded, applied and
/// committed before the loop returns, so a rolling restart never re-reads a
/// window this tail had already taken. Only a poll still in flight is dropped,
/// and that one committed nothing. Without the token the loop is unconditional
/// and a supervisor that waits for it -- `SupervisedTasks::shutdown` does --
/// waits until the orchestrator's grace period runs out and `SIGKILL`s the
/// role.
///
/// # Errors
/// Returns an error when the consumer cannot be built, polled, decoded, or committed.
pub async fn run_wal_tail_with_topic(
    store: WalTailProfileStore,
    config: WalTailConfig,
    shutdown: CancellationToken,
) -> Result<(), ProfilesError> {
    let WalTailConfig {
        bootstrap,
        group_id,
        wal_topic,
        poll_timeout,
        client_dispatch_queue_capacity,
        client_frame_max,
        metrics,
    } = config;
    // Raced against the token rather than awaited: an unreachable broker makes
    // this connect take as long as it takes, and a shutdown that arrives
    // meanwhile has nothing to cancel otherwise.
    let mut consumer = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(()),
        built = Consumer::builder()
            .bootstrap(bootstrap)
            .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
            .frame_max(client_frame_max.size())
            .group_id(group_id)
            .subscribe(vec![wal_topic])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build() => built.map_err(|err| {
                ProfilesError::Wal(format!("hot WAL-tail consumer build failed: {err}"))
            })?,
    };

    loop {
        let polled = tokio::select! {
            biased;
            () = shutdown.cancelled() => None,
            polled = consumer.poll(poll_timeout) => Some(
                polled
                    .inspect_err(|_| metrics.record_poll_failure())
                    .map_err(|err| {
                        ProfilesError::Wal(format!("hot WAL-tail consumer poll failed: {err}"))
                    })?,
            ),
        };
        let Some(records) = polled else {
            return Ok(());
        };
        metrics.record_poll(&records);
        // Decoded outside the store's write lock, then applied as one batch:
        // the store copies itself on write while a query holds a snapshot, and
        // a batch pays that once instead of once per record.
        let decoded = records
            .iter()
            .filter_map(|record| record.value.as_deref())
            .map(ProfileRecord::decode)
            .collect::<Result<Vec<_>, _>>()?;
        store.append_records(decoded)?;
        consumer
            .commit_sync()
            .await
            .map_err(|err| ProfilesError::Wal(format!("hot WAL-tail commit failed: {err}")))?;
        // Checked after the commit, never between the poll and it: a batch
        // this tail has already applied must reach the broker as a committed
        // offset even when the signal lands mid-iteration.
        if shutdown.is_cancelled() {
            return Ok(());
        }
    }
}
