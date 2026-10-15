use super::{
    Arc, BlockStore, EngineOpts, InMemoryMetricStore, MetricBlockStore, MetricStore,
    MetricsServiceError, ObjectStore, PrometheusApiState, QueryFrontendOptions, RefreshingMetricBlockStore,
    Router, Url, WalHead, load_compaction_manifests, prometheus_api_state_for_store,
    prometheus_router,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn blockstore_prometheus_router(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: &str,
) -> Result<Router, MetricsServiceError> {
    let manifests = load_compaction_manifests(store.clone(), manifest_prefix).await?;
    let metric_store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(store.clone(), base.clone()),
        Some(BlockStore::new(store, base)),
        &manifests,
    );
    Ok(prometheus_router_for_store(metric_store))
}

pub fn in_memory_prometheus_router() -> Router {
    prometheus_router_for_store(InMemoryMetricStore::new())
}

pub fn prometheus_router_for_store<S>(store: S) -> Router
where
    S: MetricStore + 'static,
{
    prometheus_router(prometheus_api_state_for_store(store))
}

pub fn query_frontend_prometheus_router_for_store<S>(store: S, opts: QueryFrontendOptions) -> Router
where
    S: MetricStore + 'static,
{
    prometheus_router(Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(opts),
    ))
}

pub fn query_frontend_prometheus_router_for_store_with_cache<S, C>(
    store: S,
    opts: QueryFrontendOptions,
    cache: C,
) -> Router
where
    S: MetricStore + 'static,
    C: krabka_promql::RangeQueryCache + 'static,
{
    prometheus_router(Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_frontend_cache(opts, Arc::new(cache)),
    ))
}

pub fn refreshing_blockstore_prometheus_router(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: impl Into<String>,
) -> Router {
    refreshing_blockstore_prometheus_router_with_hot_store(
        store,
        base,
        manifest_prefix,
        WalHead::new(),
    )
}

pub fn refreshing_blockstore_prometheus_router_with_hot_store(
    store: Arc<dyn ObjectStore>,
    base: Url,
    manifest_prefix: impl Into<String>,
    hot_store: WalHead,
) -> Router {
    prometheus_router_for_store(RefreshingMetricBlockStore::new(
        store,
        base,
        manifest_prefix,
        hot_store,
    ))
}
