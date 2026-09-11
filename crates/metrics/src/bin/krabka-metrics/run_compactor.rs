use krabka_observability::{CancellationToken, CriticalTaskError, SupervisedTasks};

use super::{
    Cli, ClientFrameMax, ConnectionDispatchQueueCapacity, MetricsCompactorConfig, RoleReadiness,
    ServiceMetrics, Time, TimeExt, build_object_store, run_compactor_consumer_loop,
    spawn_retention_sweeper,
};

// cargo-mutants: live compactor I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
) -> Result<(), Box<dyn std::error::Error>> {
    // The compactor serves no data port, so `/ready` on the admin port is the
    // only place an orchestrator can ask. It is ready once it holds the two
    // things it compacts between: the object store and the WAL consumer.
    let object_store_gate = readiness.gate("object-store");
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let store = build_object_store(&cli.object_store_url, metrics.object_store.clone())?;
    object_store_gate.mark_ready();
    let retention = cli.compactor_retention;
    let sweep_interval = cli.compactor_retention_sweep_interval;
    let mut config = MetricsCompactorConfig::new(cli.bootstrap);
    config.client_dispatch_queue_capacity =
        ConnectionDispatchQueueCapacity::new(cli.client_dispatch_queue_capacity)
            .expect("validated metrics client dispatch queue capacity");
    config.client_frame_max =
        ClientFrameMax::try_from(cli.client_frame_max).expect("validated metrics frame maximum");
    config.group_id = cli.compactor_group_id;
    config.client_id = cli.compactor_client_id;
    config.poll_timeout = cli.compactor_poll_timeout;
    config.flush_max_rows = cli.compactor_flush_max_rows;
    config.flush_max_age = cli.compactor_flush_max_age;
    let runtime = config.build_runtime(store.clone(), metrics.object_store.clone())?;
    let mut consumer = config.build_consumer().await?;
    wal_consumer_gate.mark_ready();
    let stopping = CancellationToken::new();
    let mut tasks = SupervisedTasks::new(stopping.clone());
    if retention > Time::ZERO {
        tasks.adopt(
            "metrics compactor retention sweeper",
            spawn_retention_sweeper(store, retention, sweep_interval, stopping.clone()),
        );
    }
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
    // The block counter moves inside the flush, so a running compactor reports
    // what it has written rather than only what it wrote before it stopped.
    tracing::info!(
        polls = result.polls,
        polled_records = result.polled_records,
        compacted_records = result.compacted_records,
        writes = result.writes,
        "metrics compactor stopped"
    );
    Ok(())
}
