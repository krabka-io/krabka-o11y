use krabka_observability::{CancellationToken, CriticalTaskError, SupervisedTasks};

use super::{
    Arc, Cli, ClientFrameMax, ClientSecurity, ConnectionDispatchQueueCapacity, Limits,
    MetricsCompactorConfig, OverridesProvider, RoleReadiness, ServiceMetrics, build_object_store,
    load_runtime_overrides, run_compactor_consumer_loop, spawn_retention_sweeper,
};

// cargo-mutants: live block-builder I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run_block_builder(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    wal_security: Option<ClientSecurity>,
) -> Result<(), Box<dyn std::error::Error>> {
    // The block builder serves no data port, so `/ready` on the admin port is
    // the only place an orchestrator can ask. It is ready once it holds the
    // two things it writes between: the WAL consumer and the object store.
    let object_store_gate = readiness.gate("object-store");
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let store = build_object_store(&cli.object_store_url, metrics.object_store.clone())?;
    object_store_gate.mark_ready();
    // The same runtime overrides file the distributor reads. It holds the
    // retention window of every tenant, and without it every tenant keeps its
    // blocks forever -- the built-in window is zero, which means "keep".
    let overrides = Arc::new(
        load_runtime_overrides(cli.runtime_overrides.as_deref())?
            .unwrap_or_else(|| OverridesProvider::new(Limits::default())),
    );
    let sweep_interval = cli.block_builder_retention_sweep_interval;
    let mut config = MetricsCompactorConfig::new(cli.bootstrap);
    config.client_dispatch_queue_capacity =
        ConnectionDispatchQueueCapacity::new(cli.client_dispatch_queue_capacity)
            .expect("validated metrics client dispatch queue capacity");
    config.client_frame_max =
        ClientFrameMax::try_from(cli.client_frame_max).expect("validated metrics frame maximum");
    config.group_id = cli.block_builder_group_id;
    config.client_id = cli.block_builder_client_id;
    config.poll_timeout = cli.block_builder_poll_timeout;
    config.flush_max_rows = cli.block_builder_flush_max_rows;
    config.flush_max_age = cli.block_builder_flush_max_age;
    let runtime = config.build_runtime(store.clone(), metrics.object_store.clone())?;
    let mut consumer = config
        .build_consumer(&metrics.wal_consumer, wal_security)
        .await?;
    wal_consumer_gate.mark_ready();
    let stopping = CancellationToken::new();
    let mut tasks = SupervisedTasks::new(stopping.clone());
    // Always, and not only where a tenant has a retention window. Retention and
    // orphan reconciliation are two halves of one pass, and the orphan half is
    // the only thing in metrics that reclaims a block whose writer died before
    // it published the manifest. A deployment with no window configured has
    // those orphans too, and gating the sweeper on the window leaked every one
    // of them forever. A pass with no window expires nothing.
    tasks.adopt(
        "metrics block-builder retention sweeper",
        spawn_retention_sweeper(store, overrides, sweep_interval, stopping.clone()),
    );
    let signal = stopping.clone();
    // Not supervised: this task is meant to finish, and finishing is how it
    // does its job.
    tokio::spawn(async move {
        krabka_observability::shutdown_signal().await;
        signal.cancel();
    });
    let stop = stopping.clone();
    let result = tokio::select! {
        result = run_compactor_consumer_loop(
            &mut consumer,
            &runtime.block_writer,
            &runtime.index_sink,
            runtime.loop_config,
            move |_| stop.is_cancelled(),
            &metrics,
        ) => result?,
        name = tasks.first_unexpected_exit() => {
            tasks.shutdown().await;
            return Err(CriticalTaskError(name).into());
        }
    };
    tasks.shutdown().await;
    // The block counter moves inside the flush, so a running block builder
    // reports what it has written rather than only what it wrote before it
    // stopped.
    tracing::info!(
        polls = result.polls,
        polled_records = result.polled_records,
        compacted_records = result.compacted_records,
        writes = result.writes,
        "metrics block builder stopped"
    );
    Ok(())
}
