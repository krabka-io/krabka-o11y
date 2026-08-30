use super::{
    Arc, BlockCatalog, QuerierBackend, QueryFrontend, Router, echo, get, query_instant,
    query_range, ready, search, search_tag_values_v2, search_tags_v2, trace_by_id,
};

/// Build the query-frontend router for any backend/catalog pair.
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    Router::new()
        .route("/api/echo", get(echo))
        .route("/ready", get(ready))
        .route("/status", get(ready))
        .route("/api/search", get(search::<B, C>))
        .route("/api/v2/traces/{trace_id}", get(trace_by_id::<B, C>))
        .route("/api/v2/search/tags", get(search_tags_v2::<B, C>))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(search_tag_values_v2::<B, C>),
        )
        .route("/api/metrics/query_range", get(query_range::<B, C>))
        .route("/api/metrics/query", get(query_instant::<B, C>))
        .with_state(qf)
}
