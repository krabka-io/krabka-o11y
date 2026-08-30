use super::{Arc, ArcSwap, BlockStore, Cli, HttpConfig, IndexedLiveSource, KrabkaSpanStore, LiveStore, LiveTier, Parser, RwLock, SharedTraceIndex, TraceIndex, TraceqlEngine, Url, engine_opts_from_cli, live_span_batches, trace_querier};

pub(crate) fn build_live_store_router(
    cli: &Cli,
    live_store: Arc<RwLock<LiveStore>>,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
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
    let tempo_router = trace_querier::http::router_with_config(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
            tag_query_filter_autocomplete_limit: cli.tag_query_filter_autocomplete_limit,
            ..HttpConfig::default()
        },
    );
    let internal_router = axum::Router::new()
        .route(
            "/api/krabka/live/span-batches",
            axum::routing::get(live_span_batches),
        )
        .with_state(live_store);
    Ok(tempo_router.merge(internal_router))
}
