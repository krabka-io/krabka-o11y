use super::{
    QUERIER_OPS, QuerierState, RoleReadiness, Router, loki_query_routes, with_role_ops_routes,
};

pub(crate) fn loki_router_with_readiness(state: QuerierState, readiness: RoleReadiness) -> Router {
    with_role_ops_routes(Router::new(), QUERIER_OPS, readiness).merge(loki_query_routes(state))
}
