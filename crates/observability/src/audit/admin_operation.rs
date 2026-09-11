use super::{AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, AuditResource, EpochMs};

/// An event for an operation that changes a tenant's data or the service
/// itself, at `time`.
///
/// `operation` is one of [`OPERATIONS`](super::OPERATIONS). Every resource
/// type in `resources` is one of [`RESOURCE_TYPES`](super::RESOURCE_TYPES).
/// Use [`AuditOutcome::Failure`] when the service tried the operation and the
/// operation failed. Record an authorization refusal with
/// [`authorization_denied`](super::authorization_denied).
#[must_use]
pub fn admin_operation(
    principal: AuditPrincipal,
    source: AuditEndpoint,
    operation: &'static str,
    resources: Vec<AuditResource>,
    outcome: AuditOutcome,
    time: EpochMs,
) -> AuditEvent {
    AuditEvent::AdminOperation {
        outcome,
        principal,
        source,
        operation: operation.to_owned(),
        resources,
        time_ms: time.into(),
    }
}
