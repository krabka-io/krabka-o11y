use super::{
    Arc, DefaultMs, Extension, HeaderMap, IntoResponse, Json, NowMs, ProfileStore, QuerierState,
    Query, RenderQuery, Response, flamebearer_json, flamegraph_dot, parse_render_query,
    parse_render_time_param, profile_error_response, tenant_from_headers, unix_now_ms,
};

pub(crate) async fn render_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    Query(query): Query<RenderQuery>,
) -> Response
where
    S: ProfileStore,
{
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return profile_error_response(err),
    };
    let (profile_type, selector) = match parse_render_query(&query.query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let now_ms = NowMs(unix_now_ms());
    let start = match parse_render_time_param(query.from.as_deref(), now_ms, DefaultMs(0)) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let end = match parse_render_time_param(query.until.as_deref(), now_ms, DefaultMs(i64::MAX)) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    match state
        .select_merge_stacktraces_grouped(
            (&tenant, &profile_type, &selector),
            (start, end),
            query.max_nodes.unwrap_or(0),
            &query.group_by,
        )
        .await
    {
        Ok(flamegraph)
            if query
                .format
                .as_deref()
                .is_some_and(|format| format.eq_ignore_ascii_case("dot")) =>
        {
            (
                [(axum::http::header::CONTENT_TYPE, "text/vnd.graphviz")],
                flamegraph_dot(&flamegraph),
            )
                .into_response()
        }
        Ok(flamegraph) => Json(flamebearer_json(flamegraph, &profile_type)).into_response(),
        Err(err) => profile_error_response(err),
    }
}
