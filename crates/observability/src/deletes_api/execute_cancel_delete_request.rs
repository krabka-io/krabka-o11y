use super::{
    CompactorDeleteState, HeaderMap, HttpQueryError, parse_cancel_delete_request_params, tenant,
};

pub(crate) fn execute_cancel_delete_request(
    state: &CompactorDeleteState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<(), HttpQueryError> {
    let tenant = tenant(headers)?.to_string();
    let request_id = parse_cancel_delete_request_params(raw_query)?;
    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .retain(|request| request.tenant != tenant || request.request_id != request_id);
    drop(requests);
    state.delete_requests.persist()?;
    Ok(())
}
