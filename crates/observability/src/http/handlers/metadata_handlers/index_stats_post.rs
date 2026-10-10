use super::{
    HttpQueryError, PostedQueryRequest, QuerierState, Response, State, StatusCode,
    execute_index_stats_query, json_response,
};

pub(crate) async fn index_stats_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    let value = execute_index_stats_query(
        &state,
        &request.security,
        &request.headers,
        Some(&raw_query),
    )
    .await?;
    Ok(json_response(StatusCode::OK, &value))
}
