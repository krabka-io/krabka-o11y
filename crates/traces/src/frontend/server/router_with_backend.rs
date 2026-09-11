use super::{
    Arc, BlockCatalog, QuerierBackend, QueryFrontend, RoleReadiness, Router, echo, get,
    query_instant, query_range, search, search_tag_values_v2, search_tags_v2,
    tempo_readiness_routes, trace_by_id,
};

/// Build the query-frontend router for any backend/catalog pair.
///
/// `readiness` is what `/ready` and `/status` report. The frontend's own gates
/// include `querier-membership`, so a frontend with nobody to fan out to is
/// reported as unready rather than as a role that answers every query with a
/// transport error.
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>, readiness: RoleReadiness) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    Router::new()
        .route("/api/echo", get(echo))
        .route("/api/search", get(search::<B, C>))
        .route("/api/v2/traces/{trace_id}", get(trace_by_id::<B, C>))
        .route("/api/v2/search/tags", get(search_tags_v2::<B, C>))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(search_tag_values_v2::<B, C>),
        )
        .route("/api/metrics/query_range", get(query_range::<B, C>))
        .route("/api/metrics/query", get(query_instant::<B, C>))
        .merge(tempo_readiness_routes(readiness))
        .with_state(qf)
}
