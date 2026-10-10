use super::{
    HttpQueryError, PostedQueryRequest, QuerierState, Response, StatusCode, VolumeKind,
    execute_index_volume_query, json_response,
};

/// Answers a `POST` to `/index/volume` or `/index/volume_range`, whose form
/// body takes precedence over its URL query string.
pub(crate) async fn posted_index_volume_response(
    state: &QuerierState,
    request: &PostedQueryRequest,
    kind: VolumeKind,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    let value = execute_index_volume_query(
        state,
        &request.security,
        &request.headers,
        Some(&raw_query),
        kind,
    )
    .await?;
    Ok(json_response(StatusCode::OK, &value))
}
