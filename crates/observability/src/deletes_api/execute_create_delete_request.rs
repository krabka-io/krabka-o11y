use super::*;

pub(crate) fn execute_create_delete_request(
    state: &CompactorDeleteState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<(), HttpQueryError> {
    let tenant = tenant(headers)?.to_string();
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
        tenant,
        request_id,
        query: params.query,
        start_time: params.start_time,
        end_time: params.end_time,
        status: "received".to_string(),
        created_at: current_unix_time_ns() / 1_000_000_000,
    });
    drop(requests);
    state.delete_requests.persist()?;
    Ok(())
}
