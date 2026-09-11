use super::{
    AppState, Extension, HeaderMap, Path, Principal, Response, SpanStore, State, Uri,
    trace_by_id_inner,
};

pub(crate) async fn trace_by_id<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = trace_by_id_inner(&state, &principal, headers, trace_id, uri).await;
    state.record_query("trace_by_id", resp.status().is_success(), start);
    resp
}
