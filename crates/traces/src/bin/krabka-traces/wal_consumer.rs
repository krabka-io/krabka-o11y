use super::{AutoOffsetReset, Cli, ClientSecurity, Consumer, TRACES_WAL_TOPIC};

/// A consumer of the traces WAL in `group_id`, with the fetch and client
/// limits of `cli`, that connects to the broker with `security`.
pub(crate) async fn wal_consumer(
    cli: &Cli,
    group_id: &str,
    group_instance_id: Option<&str>,
    security: Option<&ClientSecurity>,
) -> Result<Consumer, krabka_client_consumer::ConsumerError> {
    // Boxed: consumer startup (bootstrap resolve, double `JoinGroup`,
    // `SyncGroup`, offset priming) builds a ~13 KB future. Every role that
    // reads the WAL awaits this, so leaving it inline pushes each role future
    // — and the `run` dispatcher that unions them — past `clippy::large_futures`.
    // The consumer is built once per process, so the allocation is free.
    Box::pin(
        Consumer::builder()
            .bootstrap(cli.bootstrap.clone())
            .group_id(group_id.to_string())
            .maybe_group_instance_id(group_instance_id)
            .fetch_max(cli.wal_fetch_max)
            .fetch_partition_max(cli.wal_fetch_partition_max)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .maybe_security(security.cloned())
            .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build(),
    )
    .await
}
