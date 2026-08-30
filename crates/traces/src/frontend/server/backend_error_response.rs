use super::{BackendError, IntoResponse, Response, StatusCode};

/// Render a propagated backend failure as the client response.
///
/// This keeps the upstream querier's status code and error text. An invalid
/// `TraceQL` query therefore surfaces as the querier's `4xx` body, not as a
/// silent empty `200`.
pub(crate) fn backend_error_response(err: &BackendError) -> Response {
    let (status, body) = err.to_http();
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    (code, body).into_response()
}
