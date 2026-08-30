use super::{
    Arc, EngineOpts, MetricStore, PrometheusApiState, QueryFrontendOptions, Router,
    prometheus_router,
};

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
