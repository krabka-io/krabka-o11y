use super::{
    AuditOutcome, DistributorState, IntoResponse, OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET,
    RequestSecurity, Response, State, StatusCode,
};

/// `DELETE /ingester/prepare_shutdown`: cancel a drain, as `Loki` does.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403 with the readiness unchanged. The audit trail records
/// each request that restores the gate.
pub(crate) async fn unset_prepare_shutdown(
    State(state): State<DistributorState>,
    security: RequestSecurity,
) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    state.prepare_shutdown.mark_ready();
    security.admin_operation(
        OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET,
        vec![security.ingester()],
        AuditOutcome::Success,
    );
    StatusCode::NO_CONTENT.into_response()
}
