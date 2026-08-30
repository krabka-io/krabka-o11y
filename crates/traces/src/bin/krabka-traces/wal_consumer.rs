use super::{AutoOffsetReset, ByteSize, Consumer, TRACES_WAL_TOPIC};

pub(crate) async fn wal_consumer(
    bootstrap: String,
    group_id: &str,
    group_instance_id: Option<&str>,
    fetch_max: ByteSize,
    fetch_partition_max: ByteSize,
    client_dispatch_queue_capacity: usize,
    client_frame_max: ByteSize,
) -> Result<Consumer, krabka_client_consumer::ConsumerError> {
    // Boxed: consumer startup (bootstrap resolve, double `JoinGroup`,
    // `SyncGroup`, offset priming) builds a ~13 KB future. Every role that
    // reads the WAL awaits this, so leaving it inline pushes each role future
    // — and the `run` dispatcher that unions them — past `clippy::large_futures`.
    // The consumer is built once per process, so the allocation is free.
    Box::pin(
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id.to_string())
            .maybe_group_instance_id(group_instance_id)
            .fetch_max(fetch_max)
            .fetch_partition_max(fetch_partition_max)
            .dispatch_queue_capacity(client_dispatch_queue_capacity)
            .frame_max(client_frame_max)
            .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build(),
    )
    .await
}
