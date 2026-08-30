use super::{SpanStore, Arc, TraceqlEngine, HttpConfig, ServiceMetrics, Router, router_with_state, AppState};

/// Like [`router_with_config`], but it also threads a [`ServiceMetrics`]
/// bundle. Each query handler then records `query_requests` and
/// `query_duration_seconds`.
pub fn router_with_config_and_metrics<S>(
    engine: Arc<TraceqlEngine<S>>,
    cfg: HttpConfig,
    metrics: ServiceMetrics,
) -> Router
where
    S: SpanStore + 'static,
{
    router_with_state(AppState {
        engine,
        cfg,
        metrics: Some(metrics),
    })
}
