use super::{
    Arc, EngineOpts, MetricStore, PrometheusApiState, QueryFrontendOptions, Router,
    prometheus_router,
};

pub fn query_frontend_prometheus_router_for_store<S>(store: S, opts: QueryFrontendOptions) -> Router
where
    S: MetricStore + 'static,
{
    prometheus_router(Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(opts),
    ))
}
