use super::{
    HeaderMap, IntoResponse, Path, QuerierState, RawQuery, RequestSecurity, Response, State,
    execute_api_prom_label_names_query, parse_series_params,
};

pub(crate) async fn api_prom_label_values(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(_name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &security, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
