use super::*;

pub(crate) async fn detected_field_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_field_values_query(&state, &headers, &name, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
