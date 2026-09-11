use krabka_observability::wal_consumer_metrics::WalConsumerMetrics;

use super::{Cli, WalTailProfileStore};

/// Runs the hot WAL tail, returning its handle for the caller to supervise.
///
/// The tail is the querier's recent window. If it stops, the role keeps
/// answering from the cold blocks alone and the last few minutes of profiles
/// are simply absent from the answer, so any end of this task -- an error or a
/// panic -- has to end the role.
pub(crate) fn spawn_wal_tail(
    cli: &Cli,
    hot: WalTailProfileStore,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
    metrics: WalConsumerMetrics,
) -> tokio::task::JoinHandle<()> {
    let config = krabka_profiles::hot_store::WalTailConfig {
        bootstrap: cli.bootstrap.clone(),
        group_id: cli.query_wal_tail_group_id.clone(),
        wal_topic: cli.wal_topic.clone(),
        poll_timeout: cli.wal_poll_timeout,
        client_dispatch_queue_capacity,
        client_frame_max,
        metrics,
    };
    tokio::spawn(async move {
        if let Err(error) = krabka_profiles::hot_store::run_wal_tail_with_topic(hot, config).await {
            tracing::error!(%error, "profiles WAL tail stopped");
        }
    })
}
