use super::{
    AppState, Extension, HeaderMap, Principal, Response, SpanStore, State, Uri, query_instant_inner,
};

pub(crate) async fn query_instant<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_instant_inner(&state, &principal, headers, uri).await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}
