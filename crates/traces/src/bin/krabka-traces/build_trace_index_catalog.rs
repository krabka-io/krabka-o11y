use super::*;

/// Build the production block catalog from the trace index when row-group
/// sharding is enabled, that is when `--target-bytes-per-job > 0`.
///
/// Otherwise this returns an empty catalog, which gives a whole-tier search
/// with no per-block fan-out.
///
/// `gates` is `Some` only in the sharding case, because that is the only case
/// that reads the object store or the index. A role with no gates to clear
/// reports nothing it does not do.
pub(crate) async fn build_trace_index_catalog(
    cli: &Cli,
    metrics: &ServiceMetrics,
    gates: Option<&BlockStoreGates>,
    object_store: &SharedObjectStore,
) -> Result<TraceIndexCatalog, Box<dyn std::error::Error + Send + Sync>> {
    if cli.target_bytes_per_job == ByteSize::from_bytes(0) {
        return Ok(TraceIndexCatalog::new(std::collections::BTreeMap::new()));
    }
    let configured = object_store.get(cli, metrics.object_store.clone()).await?;
    if let Some(gates) = gates {
        gates.object_store.mark_ready();
    }
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let trace_index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    if let Some(gates) = gates {
        gates.trace_index.mark_ready();
    }
    let blocks =
        BlockStore::new_with_block_read_max(configured.store, configured.root, cli.block_read_max);
    Ok(TraceIndexCatalog::from_trace_index(&blocks, &trace_index).await?)
}
