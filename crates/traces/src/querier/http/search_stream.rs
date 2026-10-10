use super::{IntoResponse, QuerierRequest, Response, SpanStore, search_inner};

pub(crate) async fn search_stream<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let response = search_inner(&request).await;
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
