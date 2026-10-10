use super::{
    Bytes, HeaderMap, IntoResponse, LabelValuesRequest, Path, QuerierState, RawQuery,
    RequestSecurity, Response, State, execute_label_values_query, parse_posted_series_params,
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
    match execute_label_values_query(
        &state,
        LabelValuesRequest {
            security: &security,
            headers: &headers,
            label_name: &name,
            series_params: &params,
        },
    )
    .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
