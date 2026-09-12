use super::{PushError, RequestTenantError, Status, status_from_http_status};

/// The gRPC status a push failure reaches the client as.
///
/// The errors that carry an HTTP status share one mapping rather than each
/// repeating it as a match guard. Three of those guards could never match:
/// wire errors only ever report 400 or 415, and OTLP errors only 400, so the
/// arms testing them for 429 and 500 were unreachable. Going through the
/// status code keeps the intent, applies it to all three uniformly, and stays
/// correct if any of them gains a new code.
///
/// A missing or invalid tenant is `Unauthenticated`, the gRPC analogue of the
/// 401 that Mimir answers over HTTP. A principal that is not granted the tenant
/// is `PermissionDenied`, the gRPC analogue of a 403.
pub(crate) fn status_from_push_error(error: &PushError) -> Status {
    let message = error.to_string();
    match error {
        PushError::MissingPrincipal | PushError::Produce(_) | PushError::ProduceBatch(_) => {
            Status::internal(message)
        }
        PushError::Limit(limit) => status_from_http_status(limit.http_status(), message),
        PushError::Wire(wire) => status_from_http_status(wire.status_code(), message),
        PushError::Otlp(otlp) => status_from_http_status(otlp.status_code(), message),
        PushError::Tenant(RequestTenantError::Resolve(_)) => Status::unauthenticated(message),
        PushError::Tenant(tenant) => {
            status_from_http_status(tenant.http_status().as_u16(), message)
        }
        PushError::Denied(_) => Status::permission_denied(message),
        PushError::Clock(_) | PushError::TooOldSample { .. } | PushError::TooFarInFuture { .. } => {
            Status::invalid_argument(message)
        }
    }
}
