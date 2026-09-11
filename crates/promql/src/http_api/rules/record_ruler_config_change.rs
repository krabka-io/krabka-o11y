use super::{
    AuditHandle, AuditOutcome, AuditResource, ConnectInfo, Extension, PeerAddr, Principal,
    Response, audit_principal_of, source_endpoint, unknown_source_endpoint,
};

/// Records one ruler config mutation in the audit trail, with the outcome of `response`.
///
/// A handler calls it only after the principal may use the tenant. The
/// server's security events already record a refused principal, so a 403
/// does not reach this function. `peer` is `None` for a request that did not
/// arrive through a listener, and the event then carries the unknown source
/// endpoint.
pub(crate) fn record_ruler_config_change(
    audit: &AuditHandle,
    principal: &Principal,
    peer: Option<Extension<ConnectInfo<PeerAddr>>>,
    operation: &'static str,
    resources: Vec<AuditResource>,
    response: &Response,
) {
    let source = peer.map_or_else(unknown_source_endpoint, |Extension(ConnectInfo(peer))| {
        source_endpoint(peer.socket)
    });
    let outcome = if response.status().is_success() {
        AuditOutcome::Success
    } else {
        AuditOutcome::Failure
    };
    audit.admin_operation(
        audit_principal_of(principal),
        source,
        operation,
        resources,
        outcome,
    );
}
