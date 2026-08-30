use super::*;

pub(crate) async fn run_block_builder(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let promoted_attrs = promoted_attrs_from_cli(&cli)?;
    let consumer = wal_consumer(
        cli.bootstrap.clone(),
        "krabka-traces-block-builder",
        Some("krabka-traces-block-builder"),
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let object_key_prefix = configured.prefix.to_string();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial_index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    let index = Arc::new(Mutex::new(initial_index));
    blockbuilder::run(
        consumer,
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
