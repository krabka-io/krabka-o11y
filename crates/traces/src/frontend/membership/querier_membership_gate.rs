use super::{QuerierMember, ReadinessGate};

/// The readiness gate a query-frontend holds while at least one querier can
/// answer.
///
/// The frontend has nowhere to send a job when no querier passes its own
/// `/ready`, and
/// [`QueryFrontend::search`](crate::frontend::QueryFrontend::search) fails
/// every query in that state. The gate makes the frontend say so on its own
/// `/ready`, so an orchestrator routes around it rather than sending it
/// queries it can only refuse.
///
/// The name is the one `/ready` prints: `not ready: querier-membership`.
pub const QUERIER_MEMBERSHIP_GATE: &str = "querier-membership";

/// Moves `gate` to match a freshly probed pool.
///
/// A pool with nobody ready takes the frontend out of rotation, and the first
/// querier to come back puts it in again. The gate therefore goes down as well
/// as up, for the same reason the queriers' own gates do.
pub(crate) fn mark_querier_membership_gate(gate: &ReadinessGate, members: &[QuerierMember]) {
    if members.iter().any(QuerierMember::is_ready) {
        gate.mark_ready();
    } else {
        gate.mark_unready();
    }
}
