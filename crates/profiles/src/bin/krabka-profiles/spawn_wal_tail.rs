use krabka_observability::wal_consumer_metrics::WalConsumerMetrics;

use super::{CancellationToken, Cli, ClientSecurity, WalTailProfileStore};

/// Runs the hot WAL tail, returning its handle for the caller to supervise.
///
/// The tail is the querier's recent window. If it stops, the role keeps
/// answering from the cold blocks alone and the last few minutes of profiles
/// are simply absent from the answer, so any end of this task -- an error or a
/// panic -- has to end the role.
///
/// `shutdown` is the role's token, and the tail must watch it: the supervisor
/// waits for every adopted task on the way out, so a tail that polls forever
/// holds the whole process open until the orchestrator kills it.
///
/// `wal_security` is the TLS and SASL of the consumer.
pub(crate) fn spawn_wal_tail(
    cli: &Cli,
    hot: WalTailProfileStore,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
    metrics: WalConsumerMetrics,
    wal_security: Option<ClientSecurity>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let config = krabka_profiles::hot_store::WalTailConfig {
        bootstrap: cli.bootstrap.clone(),
        group_id: cli.query_wal_tail_group_id.clone(),
        wal_topic: cli.wal_topic.clone(),
        poll_timeout: cli.wal_poll_timeout,
        client_dispatch_queue_capacity,
        client_frame_max,
        metrics,
        security: wal_security,
    };
    tokio::spawn(async move {
        if let Err(error) =
            krabka_profiles::hot_store::run_wal_tail_with_topic(hot, config, shutdown).await
        {
            tracing::error!(%error, "profiles WAL tail stopped");
        }
    })
}
