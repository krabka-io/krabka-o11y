use axum::{
    extract::Request,
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse as _, Response},
};

// Pyroscope's querier RPCs reject GET, including methods with no side effects.
pub(crate) async fn post_only_querier(request: Request, next: Next) -> Response {
    if request.method() != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, "POST")]).into_response();
    }
    next.run(request).await
}
