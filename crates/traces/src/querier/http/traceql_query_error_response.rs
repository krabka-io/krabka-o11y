use super::{Response, StatusCode, TraceqlError};

pub(crate) fn traceql_query_error_response(err: &TraceqlError) -> Response {
    let status = if matches!(
        &err,
        TraceqlError::Parse(_) | TraceqlError::Plan(_) | TraceqlError::Unsupported(_)
    ) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    super::status_text_response(status, err.to_string())
}
