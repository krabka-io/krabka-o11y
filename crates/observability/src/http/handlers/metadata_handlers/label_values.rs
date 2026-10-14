use super::{
    HeaderMap, Instant, IntoResponse, Path, QuerierState, RawQuery, RequestSecurity, Response,
    State, execute_label_values_response, parse_series_params,
};

pub(crate) async fn label_values(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => {
            execute_label_values_response(&state, &security, &headers, &name, &params).await
        }
        Err(error) => error.into_response(),
    };
    state.record_query("label_values", resp.status(), start);
    resp
}
