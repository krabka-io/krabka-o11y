use super::{
    AppState, RoleReadiness, Router, SpanStore, buildinfo, echo, get, overrides, query_instant,
    query_range, search, search_stream, search_tag_values, search_tag_values_v2, search_tags,
    search_tags_v2, tempo_readiness_routes, trace_by_id, trace_by_id_v1,
};
use crate::tempo_query_routes::tempo_query_routes;

pub(crate) fn router_with_state<S>(state: AppState<S>, readiness: RoleReadiness) -> Router
where
    S: SpanStore + 'static,
{
    tempo_query_routes!(
        Router::new();
        echo = get(echo),
        buildinfo = get(buildinfo),
        overrides = get(overrides::<S>),
        search = get(search::<S>),
        search_stream = get(search_stream::<S>),
        trace_v1 = get(trace_by_id_v1::<S>),
        trace_v2 = get(trace_by_id::<S>),
        tags_v1 = get(search_tags::<S>),
        tags_v2 = get(search_tags_v2::<S>),
        tag_values_v1 = get(search_tag_values::<S>),
        tag_values_v2 = get(search_tag_values_v2::<S>),
        metrics_range = get(query_range::<S>),
        metrics_instant = get(query_instant::<S>),
    )
    .merge(tempo_readiness_routes(readiness))
    .with_state(state)
}
