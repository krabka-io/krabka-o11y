use super::{
    AutoOffsetReset, Consumer, ProfileRecord, ProfilesError, WalTailConfig, WalTailProfileStore,
};

/// Consume the configured profiles WAL topic into the hot query store.
///
/// # Errors
/// Returns an error when the consumer cannot be built, polled, decoded, or committed.
pub async fn run_wal_tail_with_topic(
    store: WalTailProfileStore,
    config: WalTailConfig,
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
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
        .frame_max(client_frame_max.size())
        .group_id(group_id)
        .subscribe(vec![wal_topic])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .map_err(|err| ProfilesError::Wal(format!("hot WAL-tail consumer build failed: {err}")))?;

    loop {
        let records = consumer
            .poll(poll_timeout)
            .await
            .inspect_err(|_| metrics.record_poll_failure())
            .map_err(|err| {
                ProfilesError::Wal(format!("hot WAL-tail consumer poll failed: {err}"))
            })?;
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
    }
}
