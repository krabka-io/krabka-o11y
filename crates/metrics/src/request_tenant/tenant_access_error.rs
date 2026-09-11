use super::{Error, IntoResponse, RequestTenantError, Response, TenantDenied};

/// Why a metrics request may not use the tenant it names.
///
/// [`authorized_tenant_from_headers`](super::authorized_tenant_from_headers)
/// returns it. Each variant keeps the response of its source error, so an
/// unusable tenant is still Mimir's plain-text rejection, and a refused
/// principal is a 403.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TenantAccessError {
    /// The request names no usable tenant.
    #[error(transparent)]
    Unresolved(#[from] RequestTenantError),

    /// The request's principal is not granted the tenant.
    #[error(transparent)]
    Denied(#[from] TenantDenied),
}

impl IntoResponse for TenantAccessError {
    fn into_response(self) -> Response {
        match self {
            Self::Unresolved(error) => error.into_response(),
            Self::Denied(error) => error.into_response(),
        }
    }
}
