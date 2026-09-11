use krabka_observability::RoleReadiness;

use super::{
    Arc, ArcSwap, BlockStore, Cli, HttpConfig, IndexedLiveSource, KrabkaSpanStore, LiveStore,
    LiveTier, RwLock, SharedTraceIndex, TenantPolicy, TraceIndex, TraceqlEngine, Url,
    engine_opts_from_cli, limits_from_cli, live_span_batches, load_traces_limits_overrides_config,
    trace_querier,
};

pub(crate) fn build_live_store_router(
    cli: &Cli,
    live_store: Arc<RwLock<LiveStore>>,
    readiness: RoleReadiness,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    // This role serves the Tempo read API beside its internal span-batch
    // route, so its read gates resolve a tenant through the same provider the
    // querier builds.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        limits_from_cli(cli),
    )?;
    let trace_index: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(TraceIndex::new()));
    let blocks = Arc::new(BlockStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Url::parse("memory:///")?,
    ));
    let live = LiveTier::new(Arc::new(IndexedLiveSource::new(
        Arc::clone(&live_store),
        Arc::clone(&trace_index),
    )));
    let store = Arc::new(KrabkaSpanStore::new_with_scan_concat_max(
        blocks,
        trace_index,
        Some(live),
        cli.scan_concat_max,
    ));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)?));
    let cfg = HttpConfig {
        max_trace_spans: cli.max_trace_spans,
        tag_query_filter_autocomplete_limit: cli.tag_query_filter_autocomplete_limit,
        overrides,
        tenant_policy: TenantPolicy::anonymous(),
    };
    // The internal route resolves its tenant with the policy the Tempo routes
    // beside it use, so the two cannot disagree on a request without a tenant.
    let tenant_policy = cfg.tenant_policy.clone();
    let tempo_router = trace_querier::http::router_with_config(engine, cfg, readiness);
    let internal_router = axum::Router::new()
        .route(
            "/api/krabka/live/span-batches",
            axum::routing::get(live_span_batches),
        )
        .with_state((live_store, tenant_policy));
    Ok(tempo_router.merge(internal_router))
}
