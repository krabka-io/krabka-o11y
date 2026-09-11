use std::net::SocketAddr;

use krabka_blockstore::TenantId;

use super::{
    AuditHandle, AuditOutcome, MECHANISM_NONE, OPERATION_ADMIN_ACCESS, OPERATION_TENANT_ACCESS,
    RESOURCE_ADMIN_API, RESOURCE_TENANT, mechanism_of, principal, source_endpoint,
    unauthenticated_principal, unknown_source_endpoint,
};
use crate::server_security::{AuthFailureReason, AuthMethod, SecurityEvents};

/// Records the security decisions of the listeners in the audit log.
///
/// A failed authentication and every refusal become audit events. A
/// successful authentication does not. Every authenticated request passes
/// through that decision, so an event for each one would fill the audit topic
/// with one record per query and push the records that matter out through the
/// queue's drop counter. The principal that succeeded is still in the trail,
/// because every admin operation it makes carries it.
///
/// [`authorize_tenant`](crate::server_security::authorize_tenant) and
/// [`authorize_admin`](crate::server_security::authorize_admin) do not know the
/// client's address, so their refusals carry
/// [`unknown_source_endpoint`](super::unknown_source_endpoint).
impl SecurityEvents for AuditHandle {
    fn authentication_failed(
        &self,
        source: Option<SocketAddr>,
        attempted: Option<AuthMethod>,
        reason: AuthFailureReason,
    ) {
        self.authentication(
            AuditOutcome::Failure,
            attempted.map_or(MECHANISM_NONE, mechanism_of),
            unauthenticated_principal(),
            source.map_or_else(unknown_source_endpoint, source_endpoint),
            Some(failure_reason(reason).to_owned()),
        );
    }

    fn authentication_succeeded(
        &self,
        _source: Option<SocketAddr>,
        _principal: &str,
        _method: AuthMethod,
    ) {
    }

    fn tenant_denied(&self, name: &str, method: AuthMethod, tenant: &TenantId) {
        self.authorization_denied(
            principal(name, mechanism_of(method)),
            unknown_source_endpoint(),
            RESOURCE_TENANT,
            tenant.as_str(),
            OPERATION_TENANT_ACCESS,
        );
    }

    fn admin_denied(&self, name: &str, method: AuthMethod) {
        self.authorization_denied(
            principal(name, mechanism_of(method)),
            unknown_source_endpoint(),
            RESOURCE_ADMIN_API,
            "",
            OPERATION_ADMIN_ACCESS,
        );
    }
}

/// A stable name for why a request failed authentication.
///
/// The name says which rule the request broke and never what it sent.
const fn failure_reason(reason: AuthFailureReason) -> &'static str {
    match reason {
        AuthFailureReason::MissingCredential => "missing_credential",
        AuthFailureReason::UnsupportedScheme => "unsupported_scheme",
        AuthFailureReason::MalformedCredential => "malformed_credential",
        AuthFailureReason::UnknownCredential => "unknown_credential",
        AuthFailureReason::AmbiguousClientCertificate => "ambiguous_client_certificate",
    }
}
