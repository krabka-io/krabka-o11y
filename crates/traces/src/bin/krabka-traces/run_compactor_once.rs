use super::{Cli, build_object_store, BlockWriter, TraceIndex, compact_index_window_with_max_bytes};

pub(crate) async fn run_compactor_once(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let mut index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    compact_index_window_with_max_bytes(
        configured.store.clone(),
        &writer,
        &mut index,
        configured.prefix.as_ref(),
        cli.compaction_start.0,
        cli.compaction_end.0,
        cli.block_read_max,
    )
    .await?;
    index
        .save_latest_snapshot_with_retain(
            &configured.store,
            &trace_index_key,
            cli.index_snapshot_retain,
        )
        .await?;
    Ok(())
}
