use super::{IntoResponse, Response, StatusCode, TracesError, tempo_error_response};

pub(crate) fn error_response(err: &TracesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            tempo_error_response(status, err.to_string())
        }
        // The same 403 body that every Krabka listener sends for a denial.
        TracesError::TenantDenied(denied) => denied.clone().into_response(),
        _ => (status, err.to_string()).into_response(),
    }
}
