use super::{AppState, HeaderMap, IntoResponse, Json, QueryEnforcer, Response, SearchOptions, SpanStore, StatusCode, Uri, duration_param, filter_search_duration, limit_error_response, optional_usize_param, required_seconds_param, scan_options_param, search_json, search_query, tenant};

pub(crate) async fn search_inner<S>(state: &AppState<S>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let query = match search_query(&uri) {
        Ok(Some(query)) => query,
        Ok(None) => {
            return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
        }
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let start_ns = match required_seconds_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end_ns = match required_seconds_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if end_ns < start_ns {
        return (StatusCode::BAD_REQUEST, "end must be >= start").into_response();
    }
    let limit = match optional_usize_param(&uri, "limit") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let limits = state.cfg.limits_for_tenant(&tenant);
    if let Err(err) =
        QueryEnforcer::check_search_limit(limits, u64::try_from(limit).unwrap_or(u64::MAX))
    {
        return limit_error_response(&err);
    }
    if let Err(err) = QueryEnforcer::check_search_duration(limits, start_ns, end_ns) {
        return limit_error_response(&err);
    }
    if limit > state.engine.max_traces() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "max traces per search exceeded",
        )
            .into_response();
    }
    let spss = match optional_usize_param(&uri, "spss") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let min_duration = match duration_param(&uri, "minDuration") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let max_duration = match duration_param(&uri, "maxDuration") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let duration_filtered = min_duration.is_some() || max_duration.is_some();
    let search_limit = duration_filtered.then_some(usize::MAX);

    match state
        .engine
        .search_with_options(
            &tenant,
            &query,
            start_ns,
            end_ns,
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
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}
