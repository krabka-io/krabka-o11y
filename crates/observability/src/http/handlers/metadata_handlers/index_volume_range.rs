use super::{
    HeaderMap, IntoResponse, QuerierState, RawQuery, Response, State, StatusCode, VolumeKind,
    execute_index_volume_query, json_response,
};

pub(crate) async fn index_volume_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_index_volume_query(&state, &headers, raw_query.as_deref(), VolumeKind::Range)
        .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
