use super::{AuditHandle, CancellationToken, JoinHandle, ServerSecurity};

/// The security of one serving role: the posture of its data port, its audit
/// handle, and the writer task behind that handle.
pub(crate) struct RuntimeSecurity {
    /// The posture that the data-port listener serves with. It reports its
    /// security events to `audit`.
    pub(crate) server: ServerSecurity,
    /// The handle that the handlers record audit events through.
    pub(crate) audit: AuditHandle,
    /// The audit writer, when the role started the audit layer itself.
    pub(crate) audit_writer: Option<JoinHandle<()>>,
    /// Stops the audit writer. Cancel it after the listener stops, so that no
    /// request records an event after the audit queue stops.
    pub(crate) audit_stop: CancellationToken,
}
