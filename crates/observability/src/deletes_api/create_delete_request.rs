use super::{
    Bytes, CompactorDeleteState, HeaderMap, HttpQueryError, RawQuery, RequestSecurity, State,
    StatusCode, authorized_delete_tenant, execute_create_delete_request,
};

pub(crate) async fn create_delete_request(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Result<StatusCode, HttpQueryError> {
    let tenant = authorized_delete_tenant(&state, &security, &headers).await?;
    execute_create_delete_request(&state, &security, &tenant, raw_query.as_deref(), &body)?;
    Ok(StatusCode::NO_CONTENT)
}
