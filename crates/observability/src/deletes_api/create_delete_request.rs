use super::{
    Bytes, CompactorDeleteState, HeaderMap, IntoResponse, RawQuery, RequestSecurity, Response,
    State, StatusCode, authorized_delete_tenant, execute_create_delete_request,
};

pub(crate) async fn create_delete_request(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let tenant = match authorized_delete_tenant(&state, &security, &headers).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    match execute_create_delete_request(&state, &security, &tenant, raw_query.as_deref(), &body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}
