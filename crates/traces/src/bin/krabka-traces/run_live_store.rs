use krabka_observability::{
    CriticalTaskError, RoleReadiness, SupervisedTasks, contain_handler_panics,
};

use super::*;

/// Holds the window of spans no block covers yet, and answers over it.
///
/// `listener` is already bound, for the same reason the querier's is: under
/// `--target all` this role takes an ephemeral loopback port and the querier
/// in the same process is pointed at whichever one it got.
pub(crate) async fn run_live_store(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The consumer is the live tier. Without it the role keeps its port and
    // answers every search from a store that stopped at the last record it
    // read, and nothing in the answer says so. The gate is registered before
    // the connect and goes back down when the loop ends.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let consumer = wal_consumer(
        cli.bootstrap.clone(),
        "krabka-traces-live-store",
        None,
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    let store = Arc::new(RwLock::new(LiveStore::new(cli.retention.nanos_i64())));
    let router = build_live_store_router(&cli, Arc::clone(&store), readiness)?;
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let live_shutdown = shutdown.clone();
    tasks.spawn("traces live-store consumer", async move {
        wal_consumer_gate.mark_ready();
        if let Err(err) = livestore::run(consumer, store, metrics, live_shutdown).await {
            tracing::error!(error = %err, "traces live-store consumer stopped");
        }
        wal_consumer_gate.mark_unready();
    });

    let bound = listener.local_addr()?;
    tracing::info!(%bound, "traces live-store listening");
    let server_shutdown = shutdown.clone();
    let server =
        axum::serve(listener, contain_handler_panics(router)).with_graceful_shutdown(async move {
            server_shutdown.cancelled().await;
        });
    let outcome = tokio::select! {
        result = server => result.map_err(Into::into),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    tasks.shutdown().await;
    outcome
}
