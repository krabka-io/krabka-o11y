use super::{
    IntoResponse, Json, QuerierRequest, QueryEnforcer, Response, SearchOptions, SpanStore,
    StatusCode, duration_param, filter_search_duration, limit_error_response, optional_usize_param,
    request_tenant, required_time_range, scan_options_param, search_json, search_query,
    status_text_response,
};

pub(crate) async fn search_inner<S>(request: &QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest {
        state,
        principal,
        headers,
        uri,
    } = request;
    let tenant = match request_tenant(headers, principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let query = match search_query(uri) {
        Ok(Some(query)) => query,
        Ok(None) => {
            return status_text_response(StatusCode::BAD_REQUEST, "missing query parameter q");
        }
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let bounds = match required_time_range(uri) {
        Ok(range) => range,
        Err(rejection) => return *rejection,
    };
    let limit = match optional_usize_param(uri, "limit") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let limits = state.cfg.limits_for_tenant(&tenant);
    if let Err(err) =
        QueryEnforcer::check_search_limit(&limits, u64::try_from(limit).unwrap_or(u64::MAX))
    {
        return limit_error_response(&err);
    }
    if let Err(err) = QueryEnforcer::check_search_duration(&limits, bounds.start_ns, bounds.end_ns)
    {
        return limit_error_response(&err);
    }
    if limit > state.engine.max_traces() {
        return status_text_response(
            StatusCode::TOO_MANY_REQUESTS,
            "max traces per search exceeded",
        );
    }
    let spss = match optional_usize_param(uri, "spss") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let min_duration = match duration_param(uri, "minDuration") {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let max_duration = match duration_param(uri, "maxDuration") {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let scan_options = match scan_options_param(uri) {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };

    let duration_filtered = min_duration.is_some() || max_duration.is_some();
    let search_limit = duration_filtered.then_some(usize::MAX);

    match state
        .engine
        .search_with_options(
            tenant.as_str(),
            &query,
            bounds.start_ns,
            bounds.end_ns,
            SearchOptions {
                limit,
                spss,
                search_limit,
                scan_options,
            },
        )
        .await
    {
        Ok(resp) => {
            let resp = if duration_filtered {
                filter_search_duration(
                    resp,
                    min_duration,
                    max_duration,
                    state.engine.effective_search_limit(limit),
                )
            } else {
                resp
            };
            Json(search_json(resp)).into_response()
        }
        Err(err) => status_text_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}
