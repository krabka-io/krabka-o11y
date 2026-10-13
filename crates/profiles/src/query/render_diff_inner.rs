use super::{
    Arc, DefaultMs, Extension, HeaderMap, IntoResponse, Json, NowMs, Principal, ProfileStore,
    QuerierState, QueryParameter, RawQuery, Response, authorized_http_tenant,
    flamebearer_diff_json, parse_render_query, parse_render_query::ParsedRenderQuery,
    profile_error_response, query_param_i64, query_param_render_time, unix_now_ms,
};

pub(crate) async fn render_diff_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response
where
    S: ProfileStore,
{
    let tenant = match authorized_http_tenant(&state, &principal, &headers) {
        Ok(tenant) => tenant,
        Err(refused) => return *refused,
    };
    let params = url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .into_owned()
        .map(|pair| QueryParameter {
            key: pair.0,
            value: pair.1,
        })
        .collect::<Vec<_>>();
    let left_query = params
        .iter()
        .find(|parameter| parameter.key == "leftQuery" || parameter.key == "query")
        .map_or("", |parameter| parameter.value.as_str());
    let right_query = params
        .iter()
        .find(|parameter| parameter.key == "rightQuery")
        .map_or(left_query, |parameter| parameter.value.as_str());
    let ParsedRenderQuery {
        profile_type: left_type,
        selector: left_selector,
    } = match parse_render_query(left_query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let ParsedRenderQuery {
        profile_type: right_type,
        selector: right_selector,
    } = match parse_render_query(right_query) {
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
            krabka_pprof::DiffQuery::new(
                krabka_pprof::ProfileSelection::new(tenant.as_str(), &left_type, &left_selector),
                krabka_pprof::MillisRange::new(left_start, left_end),
            ),
            krabka_pprof::DiffQuery::new(
                krabka_pprof::ProfileSelection::new(tenant.as_str(), &right_type, &right_selector),
                krabka_pprof::MillisRange::new(right_start, right_end),
            ),
            state.effective_max_nodes(&tenant, query_param_i64(&params, "maxNodes").unwrap_or(0)),
        )
        .await
    {
        Ok(diff) => Json(flamebearer_diff_json(diff, &left_type)).into_response(),
        Err(err) => profile_error_response(err),
    }
}
