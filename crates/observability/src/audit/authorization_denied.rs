use super::{AuditEndpoint, AuditEvent, AuditPrincipal, EpochMs};

/// An event for a request that the service refused because the principal may
/// not do `operation` on the resource, at `time`.
///
/// Use it for a WAL topic ACL refusal and for a principal that may not use
/// the tenant it names. `resource_type` is one of
/// [`RESOURCE_TYPES`](super::RESOURCE_TYPES), and `operation` is one of
/// [`OPERATIONS`](super::OPERATIONS).
#[must_use]
pub fn authorization_denied(
    principal: AuditPrincipal,
    source: AuditEndpoint,
    resource_type: &'static str,
    resource_name: impl Into<String>,
    operation: &'static str,
    time: EpochMs,
) -> AuditEvent {
    AuditEvent::AuthorizationDenied {
        principal,
        source,
        resource_type: resource_type.to_owned(),
        resource_name: resource_name.into(),
        operation: operation.to_owned(),
        time_ms: time.into(),
    }
}
