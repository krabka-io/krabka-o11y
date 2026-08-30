use super::*;

pub(crate) async fn label_names(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_label_names_query(&state, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("labels", resp.status().is_success(), start);
    resp
}
