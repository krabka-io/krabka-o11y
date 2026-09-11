use std::net::SocketAddr;

use krabka_blockstore::TenantId;

use super::{AuthFailureReason, AuthMethod};

/// Receives the security decisions that the listeners and the authorization helpers make.
///
/// An implementor records each decision, for example in an audit log. The
/// authentication middleware, the gRPC authentication layer,
/// [`authorize_tenant`](super::authorize_tenant) and
/// [`authorize_admin`](super::authorize_admin) call it on the request path, so
/// a method should return quickly and should not block on I/O. No argument ever
/// holds a token, a password, or a token digest.
///
/// A [`ServerSecurity`](super::ServerSecurity) takes an implementation through
/// [`with_security_events`](super::ServerSecurity::with_security_events).
/// [`NoSecurityEvents`](super::NoSecurityEvents) is the default.
pub trait SecurityEvents: Send + Sync + 'static {
    /// A request failed authentication and got a 401 or a gRPC `Unauthenticated`.
    ///
    /// `source` is `None` when the server has no connection information.
    /// `attempted` is `None` when the request presented no credential of a
    /// kind that Krabka reads.
    fn authentication_failed(
        &self,
        source: Option<SocketAddr>,
        attempted: Option<AuthMethod>,
        reason: AuthFailureReason,
    );

    /// A request authenticated as `principal`.
    fn authentication_succeeded(
        &self,
        source: Option<SocketAddr>,
        principal: &str,
        method: AuthMethod,
    );

    /// [`authorize_tenant`](super::authorize_tenant) refused `principal` access to `tenant`.
    ///
    /// `method` is the credential that authenticated `principal`. Only an
    /// authenticated principal can be refused, so it always has one.
    fn tenant_denied(&self, principal: &str, method: AuthMethod, tenant: &TenantId);

    /// [`authorize_admin`](super::authorize_admin) refused `principal` an admin operation.
    ///
    /// `method` is the credential that authenticated `principal`.
    fn admin_denied(&self, principal: &str, method: AuthMethod);
}
