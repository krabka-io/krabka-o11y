use krabka_observability::RoleReadiness;

use super::{
    Arc, BlockStoreGates, BlockWriter, CancellationToken, Cli, Mutex, ServiceMetrics, TraceIndex,
    blockbuilder, build_object_store, promoted_attrs_from_cli, wal_consumer,
};

pub(crate) async fn run_block_builder(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The role has no data port, so the admin port is the only place these
    // report. It still needs them: a block builder that has not reached the
    // broker writes nothing, and looks the same as one with an idle topic.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let gates = BlockStoreGates::register(&readiness);
    let promoted_attrs = promoted_attrs_from_cli(&cli)?;
    let consumer = wal_consumer(
        cli.bootstrap.clone(),
        "krabka-traces-block-builder",
        None,
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    wal_consumer_gate.mark_ready();
    let configured = build_object_store(&cli, metrics.object_store.clone())?;
    gates.object_store.mark_ready();
    let writer =
        BlockWriter::new(configured.store.clone()).with_metrics(metrics.object_store.clone());
    let object_key_prefix = configured.prefix.to_string();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial_index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    gates.trace_index.mark_ready();
    let index = Arc::new(Mutex::new(initial_index));
    blockbuilder::run(
        blockbuilder::BlockBuilderConsumer::new(consumer, &metrics.wal_consumer),
        writer,
        index,
        configured.store,
        blockbuilder::BlockBuilderConfig {
            object_key_prefix,
            index_key: trace_index_key,
            window: cli.block_builder_window,
            empty_poll_backoff: cli.block_builder_empty_poll_backoff,
            promoted_attrs,
            flush_max_records: cli.block_builder_flush_max_records,
            flush_max_age: cli.block_builder_flush_max_age,
            index_snapshot_retain: cli.index_snapshot_retain,
        },
        metrics,
        shutdown,
    )
    .await?;
    Ok(())
}
