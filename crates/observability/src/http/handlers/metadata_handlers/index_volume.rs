use super::{
    HeaderMap, Instant, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State,
    StatusCode, VolumeKind, execute_index_volume_query, json_response,
};

pub(crate) async fn index_volume(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_volume_query(
        &state,
        &security,
        &headers,
        raw_query.as_deref(),
        VolumeKind::Instant,
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_volume", resp.status().is_success(), start);
    resp
}
