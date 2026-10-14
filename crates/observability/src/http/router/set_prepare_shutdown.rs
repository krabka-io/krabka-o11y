use super::{
    DistributorState, OPERATION_INGESTER_PREPARE_SHUTDOWN_SET, RequestSecurity, Response, State,
};

/// `POST /ingester/prepare_shutdown`: take this process out of rotation.
///
/// `Loki` unregisters from the ring here so the distributors stop choosing the
/// instance. Krabka has no ring, so the equivalent -- and the thing an
/// operator's `preStop` hook actually depends on -- is to fail the readiness
/// probe. The request that reaches here therefore drops the
/// [`DRAINING_GATE`](crate::DRAINING_GATE), `/ready` answers 503 from the next
/// probe, and the orchestrator takes the endpoint out of the service. Nothing
/// is buffered to lose: a push is only acknowledged once the WAL append it
/// made has been.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403 with the readiness unchanged. The audit trail records
/// each request that drops the gate.
pub(crate) async fn set_prepare_shutdown(
    State(state): State<DistributorState>,
    security: RequestSecurity,
) -> Response {
    super::prepare_ingester_shutdown(&state, &security, OPERATION_INGESTER_PREPARE_SHUTDOWN_SET)
}
