use super::{PushError, Status, status_from_http_status};

/// The gRPC status a push failure reaches the client as.
///
/// The errors that carry an HTTP status share one mapping rather than each
/// repeating it as a match guard. Three of those guards could never match:
/// wire errors only ever report 400 or 415, and OTLP errors only 400, so the
/// arms testing them for 429 and 500 were unreachable. Going through the
/// status code keeps the intent, applies it to all three uniformly, and stays
/// correct if any of them gains a new code.
pub(crate) fn status_from_push_error(error: &PushError) -> Status {
    let message = error.to_string();
    match error {
        PushError::Produce(_) => Status::internal(message),
        PushError::Limit(limit) => status_from_http_status(limit.http_status(), message),
        PushError::Wire(wire) => status_from_http_status(wire.status_code(), message),
        PushError::Otlp(otlp) => status_from_http_status(otlp.status_code(), message),
        PushError::MissingTenant
        | PushError::InvalidTenant(_)
        | PushError::Clock(_)
        | PushError::TooOldSample { .. } => Status::invalid_argument(message),
    }
}
