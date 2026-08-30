use super::*;

// cargo-mutants: live compactor I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = build_object_store(&cli.object_store_url)?;
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
    let runtime = config.build_runtime(store.clone())?;
    let mut consumer = config.build_consumer().await?;
    let stopping = Arc::new(AtomicBool::new(false));
    if retention > Time::ZERO {
        spawn_retention_sweeper(store, retention, sweep_interval, Arc::clone(&stopping));
    }
    let signal = Arc::clone(&stopping);
    tokio::spawn(async move {
        krabka_observability::shutdown_signal().await;
        signal.store(true, Ordering::SeqCst);
    });
    let result = run_compactor_consumer_loop(
        &mut consumer,
        &runtime.block_writer,
        &runtime.index_sink,
        runtime.loop_config,
        |_| stopping.load(Ordering::SeqCst),
    )
    .await?;
    // Record the cumulative metric blocks the compactor wrote to object storage.
    metrics.record_blocks_compacted(result.writes as u64);
    tracing::info!(
        polls = result.polls,
        polled_records = result.polled_records,
        compacted_records = result.compacted_records,
        writes = result.writes,
        "metrics compactor stopped"
    );
    Ok(())
}
