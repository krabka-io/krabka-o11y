use std::sync::Arc;

use axum::Router;
use krabka_traces::frontend::{
    BlockCatalog, FrontendConfig, MembershipView, QuerierBackend, QueryFrontend,
};

pub(crate) fn build_query_frontend_app<B, C>(
    backend: B,
    catalog: C,
    config: FrontendConfig,
    membership: MembershipView,
    readiness: krabka_observability::RoleReadiness,
) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let frontend = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        config,
        membership,
    ));
    krabka_traces::frontend::server::router_with_backend(frontend, readiness)
}
