use super::*;

/// Renders the scoped tags; this is where the v1 and v2 endpoints differ.
pub(crate) type TagsRender = fn(Vec<ScopedTag>, Option<TagScope>) -> Value;

/// Answer a tag-names request.
pub(crate) async fn search_tags_inner<S>(
    request: &QuerierRequest<S>,
    render: TagsRender,
) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let bounds = match tenant_and_bounds(request.tenant_request(), optional_time_bounds) {
        Ok(tenant_window) => tenant_window,
        Err(rejection) => return *rejection,
    };
    let scope = match scope_param(uri) {
        Ok(scope) => scope,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    if let Some(query) = query_param(uri, "q")
        && !is_match_all_query(&query)
    {
        let filter = tag_filter(bounds.tenant.as_str(), &bounds, &query, uri);
        return match traces_matching_filter(state, filter).await {
            Ok(traces) => {
                Json(render(scoped_tags_from_traces(&traces, scope), scope)).into_response()
            }
            Err(rejection) => *rejection,
        };
    }
    match state
        .engine
        .tag_names(
            bounds.tenant.as_str(),
            scope,
            bounds.start_ns,
            bounds.end_ns,
        )
        .await
    {
        Ok(tags) => Json(render(tags, scope)).into_response(),
        Err(err) => status_text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

/// A tag-values request: the tag it asks about and how to render its values.
pub(crate) struct TagValuesRequest {
    pub(crate) tag: String,
    /// Renders the values; this is where the v1 and v2 endpoints differ.
    pub(crate) render: fn(&[TypedValue]) -> Value,
}

/// Answer a tag-values request.
pub(crate) async fn search_tag_values_inner<S>(
    request: &QuerierRequest<S>,
    tag_values: TagValuesRequest,
) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let TagValuesRequest { tag, render } = tag_values;
    let bounds = match tenant_and_bounds(request.tenant_request(), optional_time_bounds) {
        Ok(tenant_window) => tenant_window,
        Err(rejection) => return *rejection,
    };
    let tag = tempo_tag_alias(&tag);
    let mut expected = None;
    if let Some(query) = query_param(uri, "q")
        && !is_match_all_query(&query)
    {
        match exact_tag_value_filter(&query, tag) {
            Ok(Some(value)) => expected = Some(value),
            Ok(None) => {
                let filter = tag_filter(bounds.tenant.as_str(), &bounds, &query, uri);
                return match traces_matching_filter(state, filter).await {
                    Ok(traces) => {
                        Json(render(&tag_values_from_traces(&traces, tag))).into_response()
                    }
                    Err(rejection) => *rejection,
                };
            }
            Err(err) => return traceql_query_error_response(&err),
        }
    }
    match state
        .engine
        .tag_values(bounds.tenant.as_str(), tag, bounds.start_ns, bounds.end_ns)
        .await
    {
        Ok(values) => match expected {
            Some(expected) => Json(render(&filter_tag_values(values, &expected))).into_response(),
            None => Json(render(&values)).into_response(),
        },
        Err(err) => status_text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

/// A tag request's `q` filter.
#[derive(Clone, Copy)]
pub(crate) struct TagFilter<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) query: &'a str,
    /// Names the scan options and the autocomplete limit.
    pub(crate) uri: &'a Uri,
    pub(crate) window: TimeRange,
}

/// The traces a tag request's `q` filter selects, under the scan options and
/// the autocomplete limit the request names.
pub(crate) async fn traces_matching_filter<S>(
    state: &AppState<S>,
    filter: TagFilter<'_>,
) -> Result<Vec<TraceSpans>, Box<Response>>
where
    S: SpanStore + 'static,
{
    let TagFilter {
        tenant,
        query,
        uri,
        window,
    } = filter;
    let scan_options = scan_options_param(uri)
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    let limit = q_filter_limit(
        uri,
        state.engine.max_traces(),
        state.cfg.tag_query_filter_autocomplete_limit,
    )
    .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    matching_traces(
        state.engine.as_ref(),
        tenant,
        query,
        window.start_ns,
        window.end_ns,
        scan_options,
        limit,
    )
    .await
    .map_err(|err| Box::new(traceql_query_error_response(&err)))
}
