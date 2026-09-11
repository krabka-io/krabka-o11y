use super::{
    HeaderMap, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State, StatusCode,
    execute_detected_labels_query, json_response,
};

pub(crate) async fn detected_labels(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_labels_query(&state, &security, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
