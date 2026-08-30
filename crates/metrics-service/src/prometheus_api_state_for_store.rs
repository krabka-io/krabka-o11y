use super::{Arc, EngineOpts, MetricStore, PrometheusApiState};

pub fn prometheus_api_state_for_store<S>(store: S) -> Arc<PrometheusApiState<S>>
where
    S: MetricStore + 'static,
{
    Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ))
}
