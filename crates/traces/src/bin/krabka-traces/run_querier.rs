use krabka_observability::{
    CriticalTaskError, RoleReadiness, SupervisedTasks, contain_handler_panics,
};

use super::*;

pub(crate) async fn run_querier(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    // Registered before any of the work, and in the order the start meets it.
    // The object store, the index snapshot and the embedded live-store
    // consumer are all built before the data port binds, so the honest report
    // of this window lands on the admin port; the data port echoes the same
    // gates for the query-frontend that probes it.
    let gates = BlockStoreGates::register(&readiness);
    let live_store = cli
        .querier_live_store
        .then(|| Arc::new(RwLock::new(LiveStore::new(cli.retention.nanos_i64()))));
    // The embedded live tier is this querier's alone: the group gives each
    // replica a disjoint slice of the recent spans, and no other querier can
    // supply it. A querier that loses the consumer still answers, from a store
    // that stopped at the last record it read, so the gate goes back down when
    // the loop ends.
    let live_store_gate = live_store.is_some().then(|| readiness.gate("live-store"));
    let (router, store, trace_index_key, trace_index) = build_querier_router_with_live(
        &cli,
        metrics.clone(),
        live_store.clone(),
        &gates,
        readiness,
    )
    .await?;
    // Both loops below decide what this querier can see. Supervised, so that a
    // stop of either -- error, early return, or panic -- ends the role rather
    // than leaving it answering from a tier that no longer moves.
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    if let Some(live_store) = live_store {
        let consumer = wal_consumer(
            cli.bootstrap.clone(),
            "krabka-traces-querier-live-store",
            None,
            cli.wal_fetch_max,
            cli.wal_fetch_partition_max,
            cli.client_dispatch_queue_capacity,
            cli.client_frame_max,
        )
        .await?;
        let live_shutdown = shutdown.clone();
        let live_metrics = metrics.clone();
        tasks.spawn("traces querier embedded live-store", async move {
            if let Some(gate) = &live_store_gate {
                gate.mark_ready();
            }
            if let Err(err) =
                livestore::run(consumer, live_store, live_metrics, live_shutdown).await
            {
                tracing::error!(error = %err, "traces querier embedded live-store stopped");
            }
            if let Some(gate) = &live_store_gate {
                gate.mark_unready();
            }
        });
    }
    // Periodically reload the TraceIndex so newly-compacted blocks become visible
    // without restarting the querier.
    let refresh_shutdown = shutdown.clone();
    let refresh_store = Arc::clone(&store);
    let refresh_index = Arc::clone(&trace_index);
    let refresh_interval = cli.block_builder_window;
    let index_snapshot_max = cli.index_snapshot_max;
    tasks.spawn("traces querier index refresher", async move {
        let mut tick = tokio::time::interval(refresh_interval.to_std());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = refresh_shutdown.cancelled() => break,
                _ = tick.tick() => {
                    match TraceIndex::load_latest_snapshot_with_max_bytes(
                        &refresh_store,
                        &trace_index_key,
                        index_snapshot_max,
                    ).await {
                        Ok(index) => refresh_index.store(Arc::new(index)),
                        Err(error) => {
                            tracing::warn!(%error, %trace_index_key, "trace index refresh failed; retaining last good index");
                        }
                    }
                }
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "traces querier listening");
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
