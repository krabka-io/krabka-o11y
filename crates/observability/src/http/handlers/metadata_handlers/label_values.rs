use super::{
    HeaderMap, Instant, IntoResponse, Path, QuerierState, RawQuery, Response, State,
    execute_label_values_query, parse_series_params,
};

pub(crate) async fn label_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_label_values_query(&state, &headers, &name, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("label_values", resp.status().is_success(), start);
    resp
}
