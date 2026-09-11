use super::{
    CompactorDeleteState, HeaderMap, IntoResponse, RawQuery, RequestSecurity, Response, State,
    StatusCode, authorized_delete_tenant, execute_list_delete_requests, json, json_response,
};

pub(crate) async fn list_delete_requests(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match authorized_delete_tenant(&state, &security, &headers).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    match execute_list_delete_requests(&state, &tenant, raw_query.as_deref()) {
        Ok(requests) => json_response(StatusCode::OK, &json!(requests)),
        Err(error) => error.into_response(),
    }
}
