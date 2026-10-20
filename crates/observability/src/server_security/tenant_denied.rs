use std::sync::Arc;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use krabka_blockstore::TenantId;

/// An authenticated principal asked for a tenant outside its grant.
///
/// The response is a 403 with a plain-text body that names the principal and
/// the requested tenant. It does not list the tenants the principal may use.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("principal {principal:?} is not allowed to access tenant \"{tenant}\"")]
pub struct TenantDenied {
    /// The principal's name.
    pub principal: Arc<str>,
    /// The tenant that the request asked for.
    pub tenant: TenantId,
}

impl IntoResponse for TenantDenied {
    fn into_response(self) -> Response {
        crate::text_response(StatusCode::FORBIDDEN, &format!("{self}\n"))
    }
}
