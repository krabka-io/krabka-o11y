use super::{MetricStore, Router, prometheus_api_state_for_store, prometheus_router};

pub fn prometheus_router_for_store<S>(store: S) -> Router
where
    S: MetricStore + 'static,
{
    prometheus_router(prometheus_api_state_for_store(store))
}
