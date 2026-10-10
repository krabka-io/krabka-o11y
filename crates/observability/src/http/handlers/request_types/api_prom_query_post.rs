use super::{
    HttpQueryError, PostedQueryRequest, QuerierState, Response, State, handle_api_prom_query,
};

pub(crate) async fn api_prom_query_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    handle_api_prom_query(state, request.security, request.headers, Some(&raw_query)).await
}
