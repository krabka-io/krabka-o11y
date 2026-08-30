use super::{SpanStore, State, Path, AppState, HeaderMap, Uri, Response, trace_by_id_inner};

pub(crate) async fn trace_by_id<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = trace_by_id_inner(&state, headers, trace_id, uri).await;
    state.record_query("trace_by_id", resp.status().is_success(), start);
    resp
}
