use krabka_observability::{ReadinessGate, wal_consumer_metrics::WalConsumerMetrics};

use super::{CancellationToken, Cli, ClientSecurity, WalTailProfileStore, client_resource_policy};

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
    metrics: WalConsumerMetrics,
    catch_up: ReadinessGate,
    wal_security: Option<ClientSecurity>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(cli);
    let config = krabka_profiles::hot_store::WalTailConfig {
        bootstrap: cli.bootstrap.clone(),
        group_id: cli.query_wal_tail_group_id.clone(),
        wal_topic: cli.wal_topic.clone(),
        poll_timeout: cli.wal_poll_timeout,
        client_dispatch_queue_capacity,
        client_frame_max,
        metrics,
        catch_up: Some(catch_up),
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
