use super::{
    CompactorDeleteState, HeaderMap, IntoResponse, RawQuery, RequestSecurity, Response, State,
    StatusCode, authorized_delete_tenant, execute_cancel_delete_request,
};

pub(crate) async fn cancel_delete_request(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match authorized_delete_tenant(&state, &security, &headers).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    match execute_cancel_delete_request(&state, &security, &tenant, raw_query.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}
