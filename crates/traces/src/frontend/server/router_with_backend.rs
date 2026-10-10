use super::{
    Arc, BlockCatalog, QuerierBackend, QueryFrontend, RoleReadiness, Router, buildinfo, echo, get,
    metrics_query, overrides, search, search_tag_values, search_tags, tempo_readiness_routes,
    trace_by_id,
};
use crate::tempo_query_routes::tempo_query_routes;

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
    tempo_query_routes!(
        Router::new();
        echo = get(echo),
        buildinfo = get(buildinfo),
        overrides = get(overrides::<B, C>).post(overrides::<B, C>).patch(overrides::<B, C>).delete(overrides::<B, C>),
        search = get(search::<B, C, false>),
        search_stream = get(search::<B, C, true>),
        trace_v1 = get(trace_by_id::<B, C, true>),
        trace_v2 = get(trace_by_id::<B, C, false>),
        tags_v1 = get(search_tags::<B, C, false>),
        tags_v2 = get(search_tags::<B, C, true>),
        tag_values_v1 = get(search_tag_values::<B, C, false>),
        tag_values_v2 = get(search_tag_values::<B, C, true>),
        metrics_range = get(metrics_query::<B, C, false>),
        metrics_instant = get(metrics_query::<B, C, true>),
    )
    .merge(tempo_readiness_routes(readiness))
    .with_state(qf)
}
