use super::{
    AppState, Arc, HttpConfig, RoleReadiness, Router, SpanStore, TraceqlEngine, router_with_state,
};

/// The querier router, with `cfg` for the query limits and `readiness` for
/// what `/ready` and `/status` report.
pub fn router_with_config<S>(
    engine: Arc<TraceqlEngine<S>>,
    cfg: HttpConfig,
    readiness: RoleReadiness,
) -> Router
where
    S: SpanStore + 'static,
{
    router_with_state(
        AppState {
            engine,
            cfg,
            metrics: None,
        },
        readiness,
    )
}
