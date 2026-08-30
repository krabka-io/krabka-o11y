use super::*;

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
