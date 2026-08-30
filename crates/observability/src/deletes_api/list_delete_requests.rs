use super::*;

pub(crate) async fn list_delete_requests(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_list_delete_requests(&state, &headers, raw_query.as_deref()) {
        Ok(requests) => json_response(StatusCode::OK, &json!(requests)),
        Err(error) => error.into_response(),
    }
}
