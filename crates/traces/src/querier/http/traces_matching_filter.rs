use super::{
    AppState, IntoResponse, Response, SpanStore, StatusCode, TimeRange, TraceSpans, Uri,
    matching_traces, q_filter_limit, scan_options_param, traceql_query_error_response,
};

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
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
    let limit = q_filter_limit(
        uri,
        state.engine.max_traces(),
        state.cfg.tag_query_filter_autocomplete_limit,
    )
    .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
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
