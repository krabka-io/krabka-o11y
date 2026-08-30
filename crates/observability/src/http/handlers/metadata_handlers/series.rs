use super::*;

pub(crate) async fn series(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_series_query(&state, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("series", resp.status().is_success(), start);
    resp
}
