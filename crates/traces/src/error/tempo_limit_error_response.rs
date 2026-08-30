use super::{LimitError, Response, StatusCode, tempo_error_response};

#[must_use]
pub fn tempo_limit_error_response(err: &LimitError) -> Response {
    tempo_error_response(
        StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::BAD_REQUEST),
        err.message(),
    )
}
