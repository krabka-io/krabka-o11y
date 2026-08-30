use super::*;

pub(crate) async fn build_querier_router_with_live(
    cli: &Cli,
    metrics: ServiceMetrics,
    live_store: Option<Arc<RwLock<LiveStore>>>,
) -> Result<
    (axum::Router, Arc<dyn ObjectStore>, String, SharedTraceIndex),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let configured = build_object_store(cli)?;
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
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
        Some(LiveTier::new(Arc::new(RemoteLiveSource::new(
            Url::parse(url)?,
            Arc::clone(&trace_index),
        ))))
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
            ..HttpConfig::default()
        },
        metrics,
    );
    Ok((router, configured.store, trace_index_key, trace_index))
}
