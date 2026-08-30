use super::{
    Bytes, HeaderMap, IntoResponse, QuerierState, RawQuery, Response, State, StatusCode,
    VolumeKind, execute_index_volume_query, json_response, post_query_params_body_first,
};

pub(crate) async fn index_volume_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_volume_query(&state, &headers, Some(&raw_query), VolumeKind::Instant).await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
