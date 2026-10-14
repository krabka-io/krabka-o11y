use super::{
    Bytes, HeaderMap, IntoResponse, LabelValuesRequest, Path, QuerierState, RawQuery,
    RequestSecurity, Response, State, execute_label_values_response, parse_posted_series_params,
};

pub(crate) async fn label_values_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let params = match parse_posted_series_params(raw_query.as_deref(), &body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    execute_label_values_response(&state, &security, &headers, &name, &params).await
}
