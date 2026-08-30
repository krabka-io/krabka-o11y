use super::*;

pub fn loki_router(state: QuerierState) -> Router {
    loki_router_with_readiness(state, ServiceReadiness::ready())
}
