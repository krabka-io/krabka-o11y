use std::net::SocketAddr;

use krabka_blockstore::TenantId;

use super::{AuthFailureReason, AuthMethod, SecurityEvents};

/// A [`SecurityEvents`] that records nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSecurityEvents;

impl SecurityEvents for NoSecurityEvents {
    fn authentication_failed(
        &self,
        _source: Option<SocketAddr>,
        _attempted: Option<AuthMethod>,
        _reason: AuthFailureReason,
    ) {
    }

    fn authentication_succeeded(
        &self,
        _source: Option<SocketAddr>,
        _principal: &str,
        _method: AuthMethod,
    ) {
    }

    fn tenant_denied(&self, _principal: &str, _method: AuthMethod, _tenant: &TenantId) {}

    fn admin_denied(&self, _principal: &str, _method: AuthMethod) {}
}
