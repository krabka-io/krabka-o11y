use super::{
    HttpQueryError, PostedQueryRequest, QuerierState, Response, State, execute_series_query,
};

pub(crate) async fn series_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let params = request.series_params()?;
    execute_series_query(&state, &request.security, &request.headers, &params).await
}
