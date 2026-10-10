use super::{
    CompactorDeleteState, HeaderMap, HttpQueryError, RawQuery, RequestSecurity, State, StatusCode,
    authorized_delete_tenant, execute_cancel_delete_request,
};

pub(crate) async fn cancel_delete_request(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<StatusCode, HttpQueryError> {
    let tenant = authorized_delete_tenant(&state, &security, &headers).await?;
    execute_cancel_delete_request(&state, &security, &tenant, raw_query.as_deref())?;
    Ok(StatusCode::NO_CONTENT)
}
