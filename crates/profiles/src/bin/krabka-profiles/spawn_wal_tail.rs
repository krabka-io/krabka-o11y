use super::*;

pub(crate) fn spawn_wal_tail(
    cli: &Cli,
    hot: WalTailProfileStore,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
) -> tokio::task::JoinHandle<Result<(), krabka_profiles::ProfilesError>> {
    let bootstrap = cli.bootstrap.clone();
    let group_id = cli.query_wal_tail_group_id.clone();
    let wal_topic = cli.wal_topic.clone();
    let poll_timeout = cli.wal_poll_timeout;
    tokio::spawn(async move {
        krabka_profiles::hot_store::run_wal_tail_with_topic(
            hot,
            bootstrap,
            group_id,
            wal_topic,
            poll_timeout,
            client_dispatch_queue_capacity,
            client_frame_max,
        )
        .await
    })
}
