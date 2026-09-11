use super::{
    CompactorDeleteRequestResponse, CompactorDeleteState, HttpQueryError, TenantId,
    delete_request_overlaps_filter, parse_list_delete_requests_params,
};

pub(crate) fn execute_list_delete_requests(
    state: &CompactorDeleteState,
    tenant: &TenantId,
    raw_query: Option<&str>,
) -> Result<Vec<CompactorDeleteRequestResponse>, HttpQueryError> {
    let params = parse_list_delete_requests_params(raw_query)?;
    let requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant.as_str())
        .filter(|request| delete_request_overlaps_filter(request, &params))
        .map(|request| CompactorDeleteRequestResponse {
            request_id: request.request_id.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            query: request.query.clone(),
            status: request.status.clone(),
            created_at: request.created_at,
        })
        .collect())
}
