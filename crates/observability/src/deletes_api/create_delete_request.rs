use super::{
    Bytes, CompactorDeleteState, HeaderMap, IntoResponse, RawQuery, Response, State, StatusCode,
    execute_create_delete_request,
};

pub(crate) async fn create_delete_request(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    match execute_create_delete_request(&state, &headers, raw_query.as_deref(), &body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}
