use super::{IntoResponse, Response, StatusCode, TenantResolveError};

/// Maps a tenant that does not resolve to the HTTP response of a legacy route.
///
/// This is the plain HTTP form of [`super::tenant_connect_error`], and the
/// same reason applies: Pyroscope with multi-tenancy off does not read
/// `X-Scope-OrgID`, but Krabka isolates by tenant and so rejects a malformed
/// name. The status is 400. The body is the `dskit` message as
/// `text/plain; charset=utf-8`, which is the content type that the pinned
/// Mimir and Loki images send with a tenant error.
pub(crate) fn tenant_error_response(error: &TenantResolveError) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}
