use super::*;

/// Consume the configured profiles WAL topic into the hot query store.
///
/// # Errors
/// Returns an error when the consumer cannot be built, polled, decoded, or committed.
pub async fn run_wal_tail_with_topic(
    store: WalTailProfileStore,
    bootstrap: String,
    group_id: String,
    wal_topic: String,
    poll_timeout: Time,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
) -> Result<(), ProfilesError> {
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
        let records = consumer.poll(poll_timeout).await.map_err(|err| {
            ProfilesError::Wal(format!("hot WAL-tail consumer poll failed: {err}"))
        })?;
        for record in records {
            let Some(value) = record.value.as_deref() else {
                continue;
            };
            store.append_record(ProfileRecord::decode(value)?)?;
        }
        consumer
            .commit_sync()
            .await
            .map_err(|err| ProfilesError::Wal(format!("hot WAL-tail commit failed: {err}")))?;
    }
}
