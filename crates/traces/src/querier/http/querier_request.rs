use axum::{extract::FromRequestParts, http::request::Parts};

use super::{
    AppState, Extension, HeaderMap, IntoResponse, Principal, Response, SpanStore, TenantId,
    TenantRequest, Uri, metrics_request,
};

/// What every querier read handler extracts from its request: the app state,
/// the authenticated principal, the headers, and the URI.
pub(crate) struct QuerierRequest<S: SpanStore> {
    pub(crate) state: AppState<S>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) uri: Uri,
}

impl<S> FromRequestParts<AppState<S>> for QuerierRequest<S>
where
    S: SpanStore + 'static,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState<S>,
    ) -> Result<Self, Self::Rejection> {
        let Extension(principal) = Extension::<Principal>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            state: state.clone(),
            principal,
            headers: parts.headers.clone(),
            uri: parts.uri.clone(),
        })
    }
}

impl<S: SpanStore> QuerierRequest<S> {
    /// The tenant and the `TraceQL` metrics query this request names, or the
    /// response that rejects it.
    pub(crate) fn metrics_request(&self) -> Result<(TenantId, String), Box<Response>> {
        metrics_request(TenantRequest {
            headers: &self.headers,
            principal: &self.principal,
            policy: &self.state.cfg.tenant_policy,
            uri: &self.uri,
        })
    }
}
