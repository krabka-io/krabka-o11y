use axum::{extract::FromRequestParts, http::request::Parts};

use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Principal, QuerierBackend,
    QueryFrontend, Response, TenantRequest, Uri,
};

/// What every query-frontend read handler extracts from its request: the
/// frontend, the authenticated principal, the headers, and the URI.
pub(crate) struct FrontendRequest<B: QuerierBackend, C: BlockCatalog> {
    pub(crate) qf: Arc<QueryFrontend<B, C>>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) uri: Uri,
}

impl<B: QuerierBackend, C: BlockCatalog> FrontendRequest<B, C> {
    /// The request's tenant under the frontend's tenant policy.
    pub(crate) fn tenant_request(&self) -> TenantRequest<'_> {
        TenantRequest {
            headers: &self.headers,
            principal: &self.principal,
            policy: &self.qf.cfg.tenant_policy,
            uri: &self.uri,
        }
    }
}

impl<B, C> FromRequestParts<Arc<QueryFrontend<B, C>>> for FrontendRequest<B, C>
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        qf: &Arc<QueryFrontend<B, C>>,
    ) -> Result<Self, Self::Rejection> {
        let Extension(principal) = Extension::<Principal>::from_request_parts(parts, qf)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            qf: Arc::clone(qf),
            principal,
            headers: parts.headers.clone(),
            uri: parts.uri.clone(),
        })
    }
}
