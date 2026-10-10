use super::{
    HttpQueryError, PostedQueryRequest, QuerierState, Response, State,
    execute_api_prom_label_names_query,
};

pub(crate) async fn api_prom_label_names_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let params = request.series_params()?;
    execute_api_prom_label_names_query(&state, &request.security, &request.headers, &params).await
}
