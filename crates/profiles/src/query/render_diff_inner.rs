use super::{
    Arc, DefaultMs, Extension, HeaderMap, IntoResponse, Json, NowMs, ProfileStore, QuerierState,
    RawQuery, Response, flamebearer_diff_json, parse_render_query, profile_error_response,
    query_param_i64, query_param_render_time, tenant_from_headers, unix_now_ms,
};

pub(crate) async fn render_diff_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response
where
    S: ProfileStore,
{
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return profile_error_response(err),
    };
    let params = url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let left_query = params
        .iter()
        .find(|(name, _)| name == "leftQuery" || name == "query")
        .map_or("", |(_, value)| value.as_str());
    let right_query = params
        .iter()
        .find(|(name, _)| name == "rightQuery")
        .map_or(left_query, |(_, value)| value.as_str());
    let (left_type, left_selector) = match parse_render_query(left_query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let (right_type, right_selector) = match parse_render_query(right_query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let now_ms = NowMs(unix_now_ms());
    let global_start = match query_param_render_time(&params, "from", now_ms, DefaultMs(0)) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let global_end = match query_param_render_time(&params, "until", now_ms, DefaultMs(i64::MAX)) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let left_start =
        match query_param_render_time(&params, "leftFrom", now_ms, DefaultMs(global_start)) {
            Ok(value) => value,
            Err(err) => return profile_error_response(err),
        };
    let left_end =
        match query_param_render_time(&params, "leftUntil", now_ms, DefaultMs(global_end)) {
            Ok(value) => value,
            Err(err) => return profile_error_response(err),
        };
    let right_start =
        match query_param_render_time(&params, "rightFrom", now_ms, DefaultMs(global_start)) {
            Ok(value) => value,
            Err(err) => return profile_error_response(err),
        };
    let right_end =
        match query_param_render_time(&params, "rightUntil", now_ms, DefaultMs(global_end)) {
            Ok(value) => value,
            Err(err) => return profile_error_response(err),
        };
    if let Err(err) = state.validate_query_range(&tenant, left_start, left_end) {
        return profile_error_response(err);
    }
    if let Err(err) = state.validate_query_range(&tenant, right_start, right_end) {
        return profile_error_response(err);
    }
    match state
        .engine
        .diff(
            &tenant,
            (&left_type, &left_selector, left_start, left_end),
            (&right_type, &right_selector, right_start, right_end),
            state.effective_max_nodes(&tenant, query_param_i64(&params, "maxNodes").unwrap_or(0)),
        )
        .await
    {
        Ok(diff) => Json(flamebearer_diff_json(diff, &left_type)).into_response(),
        Err(err) => profile_error_response(err),
    }
}
