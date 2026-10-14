use super::{BackendError, Response, StatusCode, status_text_response};

/// Render a propagated backend failure as the client response.
///
/// This keeps the upstream querier's status code and error text. An invalid
/// `TraceQL` query therefore surfaces as the querier's `4xx` body, not as a
/// silent empty `200`.
pub(crate) fn backend_error_response(err: &BackendError) -> Response {
    let error = err.to_http();
    let code = StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY);
    status_text_response(code, error.body)
}
