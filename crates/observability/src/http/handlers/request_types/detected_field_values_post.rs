use super::*;

pub(crate) async fn detected_field_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_field_values_query(&state, &headers, &name, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
