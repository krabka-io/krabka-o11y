use super::{
    AuditOutcome, DistributorState, IntoResponse, OPERATION_INGESTER_SHUTDOWN, RequestSecurity,
    Response, State, StatusCode,
};

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
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    state.prepare_shutdown.mark_unready();
    security.admin_operation(
        OPERATION_INGESTER_SHUTDOWN,
        vec![security.ingester()],
        AuditOutcome::Success,
    );
    StatusCode::NO_CONTENT.into_response()
}
