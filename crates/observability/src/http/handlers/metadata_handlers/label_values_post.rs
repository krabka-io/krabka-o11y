use super::{
    Bytes, HeaderMap, IntoResponse, Path, QuerierState, RawQuery, Response, State,
    execute_label_values_query, parse_series_params, post_query_params_body_first,
};

pub(crate) async fn label_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_label_values_query(&state, &headers, &name, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
