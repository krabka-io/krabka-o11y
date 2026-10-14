use super::{DistributorState, OPERATION_INGESTER_SHUTDOWN, RequestSecurity, Response, State};

/// `/ingester/shutdown`: leave rotation for good.
///
/// `Loki` flushes, unregisters and terminates. Krabka has nothing to flush --
/// a push is acknowledged only after its WAL append is -- and no ring to leave,
/// so what is left of the operation is to stop being ready, which this does.
/// The process does not exit itself: an orchestrator that sees the probe fail
/// stops it, and a process that killed itself out from under an in-flight
/// query would be the opposite of a drain.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403 with the readiness unchanged. The audit trail records
/// each shutdown.
pub(crate) async fn shutdown_ingester(
    State(state): State<DistributorState>,
    security: RequestSecurity,
) -> Response {
    super::prepare_ingester_shutdown(&state, &security, OPERATION_INGESTER_SHUTDOWN)
}
