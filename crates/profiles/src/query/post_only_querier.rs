use axum::{
    extract::Request,
    http::{Method, StatusCode, header},
    middleware::Next,
    response::Response,
};

// Pyroscope's querier RPCs reject GET, including methods with no side effects.
pub(crate) async fn post_only_querier(request: Request, next: Next) -> Response {
    if request.method() != Method::POST {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header(header::ALLOW, "POST")
            .body(axum::body::Body::empty())
            .expect("valid method rejection response");
    }
    next.run(request).await
}
