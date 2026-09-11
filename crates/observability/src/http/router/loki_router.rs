use super::{QuerierState, RoleReadiness, Router, loki_router_with_readiness};

pub fn loki_router(state: QuerierState) -> Router {
    loki_router_with_readiness(state, RoleReadiness::new())
}
