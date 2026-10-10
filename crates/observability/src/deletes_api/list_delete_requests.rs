use super::{
    CompactorDeleteState, HeaderMap, HttpQueryError, RawQuery, RequestSecurity, Response, State,
    StatusCode, authorized_delete_tenant, execute_list_delete_requests, json, json_response,
};

pub(crate) async fn list_delete_requests(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, HttpQueryError> {
    let tenant = authorized_delete_tenant(&state, &security, &headers).await?;
    let requests = execute_list_delete_requests(&state, &tenant, raw_query.as_deref())?;
    Ok(json_response(StatusCode::OK, &json!(requests)))
}
