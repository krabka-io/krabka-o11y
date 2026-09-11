use super::{
    Bytes, HeaderMap, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State,
    handle_api_prom_query_range, post_query_params_body_first,
};

pub(crate) async fn api_prom_query_range_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    handle_api_prom_query_range(state, security, headers, Some(&raw_query)).await
}
