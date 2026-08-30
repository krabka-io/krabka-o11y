use super::{TracesError, Response, StatusCode, tempo_error_response, IntoResponse};

pub(crate) fn error_response(err: &TracesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            tempo_error_response(status, err.to_string())
        }
        _ => (status, err.to_string()).into_response(),
    }
}
