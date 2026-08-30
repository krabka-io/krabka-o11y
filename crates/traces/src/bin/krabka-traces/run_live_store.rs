use super::*;

pub(crate) async fn run_live_store(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
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
    let router = build_live_store_router(&cli, Arc::clone(&store))?;
    let live_shutdown = shutdown.clone();
    let live_failure = shutdown.clone();
    tokio::spawn(async move {
        if let Err(err) = livestore::run(consumer, store, live_shutdown).await {
            tracing::error!(error = %err, "traces live-store consumer stopped");
            live_failure.cancel();
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "traces live-store listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}
