use super::{
    CompactorDeleteState, HeaderMap, IntoResponse, RawQuery, Response, State, StatusCode,
    execute_cancel_delete_request,
};

pub(crate) async fn cancel_delete_request(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_cancel_delete_request(&state, &headers, raw_query.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}
