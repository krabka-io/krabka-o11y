use super::{
    AuditOutcome, Bytes, CompactorDeleteRequest, CompactorDeleteState, HttpQueryError,
    OPERATION_DELETE_REQUEST_CREATE, RESOURCE_DELETE_REQUEST, RESOURCE_TENANT, RequestSecurity,
    TenantId, current_unix_time_ns, parse_create_delete_request_params, parse_query,
    request_query_or_form_body, resource,
};

/// Stores a new delete request for `tenant`.
///
/// The audit trail records each request that passes validation, with the
/// request id and the outcome of the write. A request that the parameters
/// refuse changes nothing, so it records nothing.
pub(crate) fn execute_create_delete_request(
    state: &CompactorDeleteState,
    security: &RequestSecurity,
    tenant: &TenantId,
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<(), HttpQueryError> {
    let raw_params = request_query_or_form_body(raw_query, body)?;
    let params = parse_create_delete_request_params(Some(raw_params.as_str()))?;
    parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;

    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests.next_id += 1;
    let request_id = format!("delete-{}", requests.next_id);
    requests.requests.push(CompactorDeleteRequest {
        tenant: tenant.as_str().to_owned(),
        request_id: request_id.clone(),
        query: params.query,
        start_time: params.start_time,
        end_time: params.end_time,
        status: "received".to_string(),
        created_at: current_unix_time_ns() / 1_000_000_000,
    });
    drop(requests);
    let persisted = state.delete_requests.persist();
    security.admin_operation(
        OPERATION_DELETE_REQUEST_CREATE,
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
