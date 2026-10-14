use super::{
    CompactorDeleteState, HttpQueryError, OPERATION_DELETE_REQUEST_CANCEL, RequestSecurity,
    TenantId, parse_cancel_delete_request_params,
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
    let persisted = state.delete_requests.persist(&requests);
    drop(requests);
    super::audit_delete_request_write(
        security,
        OPERATION_DELETE_REQUEST_CANCEL,
        tenant,
        &request_id,
        &persisted,
    );
    persisted?;
    Ok(())
}
