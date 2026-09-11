use super::{
    HeaderMap, Instant, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State,
    execute_series_query, parse_series_params,
};

pub(crate) async fn series(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_series_query(&state, &security, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("series", resp.status().is_success(), start);
    resp
}
