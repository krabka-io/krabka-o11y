use super::{AppState, Arc, HttpConfig, Router, SpanStore, TraceqlEngine, router_with_state};

pub fn router_with_config<S>(engine: Arc<TraceqlEngine<S>>, cfg: HttpConfig) -> Router
where
    S: SpanStore + 'static,
{
    router_with_state(AppState {
        engine,
        cfg,
        metrics: None,
    })
}
