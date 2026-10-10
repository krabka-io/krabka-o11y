use axum::extract::{FromRequest, FromRequestParts, Request};

use super::{
    Arc, Bytes, DistributorState, Extension, HeaderMap, IntoResponse, Principal, Response,
};

/// What every push door extracts from its request: the distributor state,
/// the authenticated principal, the headers, and the body.
pub(crate) struct PushRequest {
    pub(crate) state: Arc<DistributorState>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl FromRequest<Arc<DistributorState>> for PushRequest {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<DistributorState>,
    ) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = request.into_parts();
        let Extension(principal) = Extension::<Principal>::from_request_parts(&mut parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let headers = parts.headers.clone();
        let body = Bytes::from_request(Request::from_parts(parts, body), state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            state: Arc::clone(state),
            principal,
            headers,
            body,
        })
    }
}
