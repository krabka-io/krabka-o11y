use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};

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
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The consumer is the live tier. Without it the role keeps its port and
    // answers every search from a store that stopped at the last record it
    // read, and nothing in the answer says so. The gate is registered before
    // the connect and goes back down when the loop ends.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let consumer = wal_consumer(
        &cli,
        "krabka-traces-live-store",
        None,
        security.wal.as_ref(),
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

    let listener = ServerListener::bind(listener, &security.server)?;
    let bound = listener.local_addr();
    tracing::info!(%bound, "traces live-store listening");
    let server = serve_router(listener, router, &security.server)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let outcome = tokio::select! {
        result = server => result.map_err(Into::into),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    tasks.shutdown().await;
    outcome
}
