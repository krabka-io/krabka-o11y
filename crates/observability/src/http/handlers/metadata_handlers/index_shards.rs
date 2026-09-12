use super::{
    HeaderMap, Instant, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State,
    StatusCode, json_response,
};
use crate::execute_index_shards_query;

pub(crate) async fn index_shards(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let response =
        match execute_index_shards_query(&state, &security, &headers, raw_query.as_deref()).await {
            Ok(value) => json_response(StatusCode::OK, &value),
            Err(error) => error.into_response(),
        };
    state.record_query("index_shards", response.status().is_success(), start);
    response
}
