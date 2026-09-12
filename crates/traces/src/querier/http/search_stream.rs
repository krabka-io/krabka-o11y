use super::{
    AppState, Extension, HeaderMap, IntoResponse, Principal, Response, SpanStore, State, Uri,
    search_inner,
};

pub(crate) async fn search_stream<S>(
    State(state): State<AppState<S>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let response = search_inner(&state, &principal, headers, uri).await;
    let status = response.status();
    let Ok(mut bytes) = axum::body::to_bytes(response.into_body(), usize::MAX).await else {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "response body failed",
        )
            .into_response();
    };
    if status.is_success() {
        bytes = [bytes.as_ref(), b"\n"].concat().into();
    }
    (status, [("content-type", "application/x-ndjson")], bytes).into_response()
}
