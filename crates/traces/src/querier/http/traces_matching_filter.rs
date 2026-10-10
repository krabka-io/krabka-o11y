use super::{
    AppState, IntoResponse, Response, SpanStore, StatusCode, TraceSpans, Uri, matching_traces,
    q_filter_limit, scan_options_param, traceql_query_error_response,
};

/// The traces a tag request's `q` filter selects, under the scan options and
/// the autocomplete limit the request names.
pub(crate) async fn traces_matching_filter<S>(
    state: &AppState<S>,
    tenant: &str,
    query: &str,
    uri: &Uri,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<TraceSpans>, Box<Response>>
where
    S: SpanStore + 'static,
{
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
        start_ns,
        end_ns,
        scan_options,
        limit,
    )
    .await
    .map_err(|err| Box::new(traceql_query_error_response(&err)))
}
