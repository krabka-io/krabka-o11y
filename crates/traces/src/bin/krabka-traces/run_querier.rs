use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};
use krabka_traces::frontend::{HttpReadinessProbe, QuerierHealth, QuerierScheme, ReadinessProbe};

use super::*;

/// Answers `TraceQL` over blocks, and over whatever live tier it can reach.
///
/// `listener` is already bound. The caller binds it because under
/// `--target all` the querier takes an ephemeral loopback port and the
/// query-frontend in the same process has to be told which one it got; a
/// listener that bound itself could only be asked after it had started
/// serving. Nothing is served on it until the startup below finishes, so a
/// probe arriving in that window waits rather than being answered wrongly.
pub(crate) async fn run_querier(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    listener: tokio::net::TcpListener,
    object_store: &SharedObjectStore,
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Registered before any of the work, and in the order the start meets it.
    // The object store, the index snapshot and the embedded live-store
    // consumer are all built before the router is served, so the honest report
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
    let remote_live_store = if live_store.is_none() && cli.target != Target::All {
        cli.querier_live_store_url
            .as_deref()
            .map(|raw| -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                let url = Url::parse(raw)?;
                let scheme = match url.scheme() {
                    "http" => QuerierScheme::Http,
                    "https" => QuerierScheme::Https,
                    scheme => return Err(format!("unsupported live-store URL scheme {scheme}").into()),
                };
                let host = url.host_str().ok_or("live-store URL has no host")?;
                let port = url.port_or_known_default().ok_or("live-store URL has no port")?;
                let addr = if host.contains(':') {
                    format!("[{host}]:{port}")
                } else {
                    format!("{host}:{port}")
                };
                Ok((
                    readiness.gate("live-store"),
                    HttpReadinessProbe::new(
                        cli.querier_readiness_timeout.to_std(),
                        scheme,
                        security.server.internal_client(),
                    )?,
                    addr,
                ))
            })
            .transpose()?
    } else {
        None
    };
    let (router, store, trace_index_key, trace_index) = build_querier_router_with_live(
        &cli,
        metrics.clone(),
        live_store.clone(),
        &gates,
        readiness,
        object_store,
        security.server.internal_client(),
    )
    .await?;
    // Both loops below decide what this querier can see. Supervised, so that a
    // stop of either -- error, early return, or panic -- ends the role rather
    // than leaving it answering from a tier that no longer moves.
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    if let Some((gate, probe, addr)) = remote_live_store {
        let probe_shutdown = shutdown.clone();
        let interval = cli.querier_membership_refresh_interval.to_std();
        tasks.spawn("traces querier remote live-store readiness", async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = probe_shutdown.cancelled() => break,
                    _ = tick.tick() => match probe.probe(&addr).await {
                        QuerierHealth::Ready => gate.mark_ready(),
                        health => {
                            gate.mark_unready();
                            tracing::warn!(%addr, ?health, "remote trace live-store is not ready");
                        }
                    }
                }
            }
        });
    }
    if let Some(live_store) = live_store {
        let consumer = wal_consumer(
            &cli,
            "krabka-traces-querier-live-store",
            None,
            security.wal.as_ref(),
        )
        .await?;
        let live_shutdown = shutdown.clone();
        let live_metrics = metrics.clone();
        tasks.spawn("traces querier embedded live-store", async move {
            let gate = live_store_gate
                .as_ref()
                .expect("embedded live store registers a gate")
                .clone();
            if let Err(err) =
                livestore::run(consumer, live_store, live_metrics, live_shutdown, gate).await
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
    let listener = ServerListener::bind(listener, &security.server)?;
    let bound = listener.local_addr();
    tracing::info!(%bound, "traces querier listening");
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
