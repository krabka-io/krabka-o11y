use krabka_observability::RoleReadiness;

use super::{
    Arc, ArcSwap, BlockStore, BlockStoreGates, Cli, HttpConfig, IndexedLiveSource, InternalClient,
    KrabkaSpanStore, LiveStore, LiveTier, ObjectStore, RemoteLiveSource, RwLock, ServiceMetrics,
    SharedObjectStore, SharedTraceIndex, TenantPolicy, TraceIndex, TraceqlEngine, Url,
    engine_opts_from_cli, limits_from_cli, load_traces_limits_overrides_config, trace_querier,
};

pub(crate) async fn build_querier_router_with_live(
    cli: &Cli,
    metrics: ServiceMetrics,
    live_store: Option<Arc<RwLock<LiveStore>>>,
    gates: &BlockStoreGates,
    readiness: RoleReadiness,
    object_store: &SharedObjectStore,
    internal_client: &InternalClient,
) -> Result<
    (axum::Router, Arc<dyn ObjectStore>, String, SharedTraceIndex),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // Built first, so that a malformed overrides file stops the role before it
    // reaches the object store.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        limits_from_cli(cli),
    )?;
    let configured = object_store.get(cli, metrics.object_store.clone()).await?;
    gates.object_store.mark_ready();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    gates.trace_index.mark_ready();
    let trace_index: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(initial));
    let blocks = Arc::new(BlockStore::new_with_block_read_max(
        Arc::clone(&configured.store),
        configured.root,
        cli.block_read_max,
    ));
    let live = if let Some(store) = live_store {
        Some(LiveTier::new(Arc::new(IndexedLiveSource::new(
            store,
            Arc::clone(&trace_index),
        ))))
    } else if let Some(url) = &cli.querier_live_store_url {
        // The live-store serves with the same security, so the querier calls it
        // as the internal principal.
        Some(LiveTier::new(Arc::new(RemoteLiveSource::new(
            Url::parse(url)?,
            Arc::clone(&trace_index),
            internal_client,
        )?)))
    } else {
        None
    };
    let store = Arc::new(KrabkaSpanStore::new_with_scan_concat_max(
        blocks,
        Arc::clone(&trace_index),
        live,
        cli.scan_concat_max,
    ));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)?));
    let router = trace_querier::http::router_with_config_and_metrics(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
            tag_query_filter_autocomplete_limit: cli.tag_query_filter_autocomplete_limit,
            overrides,
            tenant_policy: TenantPolicy::anonymous(),
        },
        metrics,
        readiness,
    );
    Ok((router, configured.store, trace_index_key, trace_index))
}
