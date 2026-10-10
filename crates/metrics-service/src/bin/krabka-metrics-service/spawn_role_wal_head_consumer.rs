use krabka_client_consumer::IsolationLevel;

use super::{
    AutoOffsetReset, Cli, ClientSecurity, Consumer, ReadinessGate, Shutdown, WalHead,
    WalHeadConsumerRecovery, spawn_wal_head_consumer_task,
};

/// Where a serving role's WAL head consumer reads from and reports to.
pub(crate) struct WalHeadFeed {
    pub(crate) bootstrap: String,
    pub(crate) security: Option<ClientSecurity>,
    pub(crate) group_id: String,
    pub(crate) head: WalHead,
    pub(crate) gate: ReadinessGate,
    pub(crate) recovery: WalHeadConsumerRecovery,
}

/// Spawns the read-committed consumer that keeps `feed.head` current with the
/// WAL topic, starting from the earliest offset its group has not committed.
pub(crate) fn spawn_role_wal_head_consumer(
    cli: &Cli,
    feed: WalHeadFeed,
    shutdown: Shutdown,
) -> tokio::task::JoinHandle<()> {
    let WalHeadFeed {
        bootstrap,
        security,
        group_id,
        head,
        gate,
        recovery,
    } = feed;
    let dispatch_queue_capacity = cli.client_dispatch_queue_capacity;
    let frame_max = cli.client_frame_max;
    let client_id = cli.wal_client_id.clone();
    let subscribe_topic = cli.wal_topic.clone();
    spawn_wal_head_consumer_task(
        move || async move {
            Consumer::builder()
                .bootstrap(bootstrap)
                .maybe_security(security)
                .dispatch_queue_capacity(dispatch_queue_capacity)
                .frame_max(frame_max)
                .group_id(group_id)
                .client_id(client_id)
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .isolation_level(IsolationLevel::ReadCommitted)
                .subscribe([subscribe_topic])
                .enable_auto_commit(false)
                .build()
                .await
                .map_err(|error| error.to_string())
        },
        head,
        cli.wal_topic.clone(),
        cli.wal_poll_timeout,
        shutdown,
        gate,
        recovery,
    )
}
