use axum::{extract::FromRequestParts, http::request::Parts};

use super::{
    HeaderMap, HttpQueryError, IntoResponse, QuerierState, RequestSecurity, Response,
    TenantErrorSurface, TenantId, authorized_tenant,
};

/// The one tenant a ruler request names, after the principal's grant and the
/// query authorizer allow it.
///
/// Loki's ruler reads the tenant with `dskit`'s `tenant.TenantID`, so a header
/// with two tenants is refused and `a|a` is tenant `a`. With `auth_enabled:
/// true`, which is the `loki_differential` configuration, a request without a
/// tenant gets 401. It is never served as tenant `fake`.
///
/// Every rule route asks the authorizer, the routes that change rule groups
/// too. An unauthenticated caller that could name any tenant could otherwise
/// read, create and delete that tenant's rules. A tenant outside the
/// principal's grant gets a 403 before the authorizer runs.
pub(crate) async fn authorized_ruler_tenant(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<TenantId, HttpQueryError> {
    authorized_tenant(state, security, headers, surface).await
}

/// The tenant of a Loki ruler request, authorized as
/// [`authorized_ruler_tenant`] does with the ruler's tenant-error surface.
pub(crate) struct RulerTenant(pub(crate) TenantId);

impl FromRequestParts<QuerierState> for RulerTenant {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &QuerierState,
    ) -> Result<Self, Self::Rejection> {
        let security = RequestSecurity::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        authorized_ruler_tenant(state, &security, &parts.headers, TenantErrorSurface::Ruler)
            .await
            .map(Self)
            .map_err(IntoResponse::into_response)
    }
}
