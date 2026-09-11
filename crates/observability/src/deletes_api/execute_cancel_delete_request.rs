use super::{
    AuditOutcome, CompactorDeleteState, HttpQueryError, OPERATION_DELETE_REQUEST_CANCEL,
    RESOURCE_DELETE_REQUEST, RESOURCE_TENANT, RequestSecurity, TenantId,
    parse_cancel_delete_request_params, resource,
};

/// Removes the delete request `request_id` of `tenant`.
///
/// The audit trail records each cancel with a valid request id, with the
/// outcome of the write. A cancel that the parameters refuse changes nothing,
/// so it records nothing.
pub(crate) fn execute_cancel_delete_request(
    state: &CompactorDeleteState,
    security: &RequestSecurity,
    tenant: &TenantId,
    raw_query: Option<&str>,
) -> Result<(), HttpQueryError> {
    let request_id = parse_cancel_delete_request_params(raw_query)?;
    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .retain(|request| request.tenant != tenant.as_str() || request.request_id != request_id);
    drop(requests);
    let persisted = state.delete_requests.persist();
    security.admin_operation(
        OPERATION_DELETE_REQUEST_CANCEL,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_DELETE_REQUEST, request_id),
        ],
        if persisted.is_ok() {
            AuditOutcome::Success
        } else {
            AuditOutcome::Failure
        },
    );
    persisted?;
    Ok(())
}
