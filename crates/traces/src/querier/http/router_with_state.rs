use super::{AppState, Router, SpanStore, buildinfo, echo, get, query_instant, query_range, ready, search, search_tag_values, search_tag_values_v2, search_tags, search_tags_v2, trace_by_id, trace_by_id_v1};

pub(crate) fn router_with_state<S>(state: AppState<S>) -> Router
where
    S: SpanStore + 'static,
{
    Router::new()
        .route("/api/echo", get(echo))
        .route("/ready", get(ready))
        .route("/status", get(ready))
        .route("/api/status/buildinfo", get(buildinfo))
        .route("/api/search", get(search::<S>))
        .route("/api/search/tags", get(search_tags::<S>))
        .route("/api/v2/search/tags", get(search_tags_v2::<S>))
        .route("/api/search/tag/{tag}/values", get(search_tag_values::<S>))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(search_tag_values_v2::<S>),
        )
        .route("/api/metrics/query_range", get(query_range::<S>))
        .route("/api/metrics/query", get(query_instant::<S>))
        .route("/api/v2/traces/{trace_id}", get(trace_by_id::<S>))
        .route("/api/traces/{trace_id}", get(trace_by_id_v1::<S>))
        .with_state(state)
}
