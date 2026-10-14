use super::{QuerierRequest, Response, SpanStore, search_inner};

pub(crate) async fn search_stream<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let response = search_inner(&request).await;
    let status = response.status();
    let Ok(mut bytes) = axum::body::to_bytes(response.into_body(), usize::MAX).await else {
        return Response::builder()
            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::from("response body failed"))
            .expect("the HTTP response builds");
    };
    if status.is_success() {
        bytes = [bytes.as_ref(), b"\n"].concat().into();
    }
    Response::builder()
        .status(status)
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from(bytes))
        .expect("the HTTP response builds")
}
