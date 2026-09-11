use super::{AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, EpochMs};

/// An event for an authentication attempt with `mechanism`, at `time`.
///
/// `mechanism` is one of [`MECHANISMS`](super::MECHANISMS). `reason` tells
/// why a failed attempt failed. It should not contain the credentials.
#[must_use]
pub fn authentication(
    outcome: AuditOutcome,
    mechanism: &'static str,
    principal: AuditPrincipal,
    source: AuditEndpoint,
    reason: Option<String>,
    time: EpochMs,
) -> AuditEvent {
    AuditEvent::Authentication {
        outcome,
        mechanism: mechanism.to_owned(),
        principal,
        source,
        reason,
        time_ms: time.into(),
    }
}
