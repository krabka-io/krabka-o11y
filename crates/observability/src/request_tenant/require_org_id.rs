use super::{
    Response, TenantErrorSurface, TenantRequestError, TenantResolveError, tenant_error_response,
    tenant_header_value,
};

/// Refuses a request without an `X-Scope-OrgID` before its handler runs.
///
/// This is `dskit`'s auth middleware. It checks only that the header is
/// present and not empty, so its 401 comes before every parameter error of the
/// handler behind it. A malformed tenant passes, and the handler refuses it in
/// the way that handler refuses errors.
pub(crate) async fn require_org_id(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if tenant_header_value(request.headers()).is_none_or(<[u8]>::is_empty) {
        return tenant_error_response(
            &TenantRequestError::Resolve(TenantResolveError::Missing),
            TenantErrorSurface::Push,
        );
    }
    next.run(request).await
}
