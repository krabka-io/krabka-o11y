use super::{
    Arc, BlockCatalog, QuerierBackend, QueryFrontend, RoleReadiness, Router, buildinfo, echo, get,
    overrides, query_instant, query_range, search, search_stream, search_tag_values,
    search_tag_values_v2, search_tags, search_tags_v2, tempo_readiness_routes, trace_by_id,
    trace_by_id_v1,
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
        overrides = get(overrides::<B, C>),
        search = get(search::<B, C>),
        search_stream = get(search_stream::<B, C>),
        trace_v1 = get(trace_by_id_v1::<B, C>),
        trace_v2 = get(trace_by_id::<B, C>),
        tags_v1 = get(search_tags::<B, C>),
        tags_v2 = get(search_tags_v2::<B, C>),
        tag_values_v1 = get(search_tag_values::<B, C>),
        tag_values_v2 = get(search_tag_values_v2::<B, C>),
        metrics_range = get(query_range::<B, C>),
        metrics_instant = get(query_instant::<B, C>),
    )
    .merge(tempo_readiness_routes(readiness))
    .with_state(qf)
}
