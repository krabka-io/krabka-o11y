use axum::{
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use krabka_blockstore::TenantId;
use krabka_observability::server_security::Principal;

use super::authorized_tenant_from_headers;

/// The headers and authenticated principal of one API request.
#[derive(Clone, Copy)]
pub(crate) struct RequestAuth<'a> {
    pub(crate) headers: &'a HeaderMap,
    pub(crate) principal: &'a Principal,
}

impl RequestAuth<'_> {
    /// The tenant the request names and its principal may read, or the error
    /// response that ends the request.
    pub(crate) fn tenant(self) -> Result<TenantId, Rejection> {
        authorized_tenant_from_headers(self.headers, self.principal).map_err(Rejection::of)
    }
}

/// The error response that ends a request, boxed so that the `Result`s
/// carrying it stay small.
pub(crate) struct Rejection(Box<Response>);

impl Rejection {
    pub(crate) fn of(error: impl IntoResponse) -> Self {
        Self(Box::new(error.into_response()))
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        *self.0
    }
}
