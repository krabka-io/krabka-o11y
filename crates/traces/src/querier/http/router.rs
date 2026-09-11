use super::{Arc, HttpConfig, RoleReadiness, Router, SpanStore, TraceqlEngine, router_with_config};

/// The querier router with the default limits, and no readiness gate.
///
/// A role with no gates is ready from construction, so `/ready` and `/status`
/// answer `200` at once. That is the honest answer for an in-process querier
/// whose whole startup already ran before the caller got the router. The role
/// binary calls [`router_with_config_and_metrics`](super::router_with_config_and_metrics)
/// with the gates its own start has to clear.
pub fn router<S>(engine: Arc<TraceqlEngine<S>>) -> Router
where
    S: SpanStore + 'static,
{
    router_with_config(engine, HttpConfig::default(), RoleReadiness::new())
}
