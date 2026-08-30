use super::{
    Bytes, HeaderMap, Instant, IntoResponse, QuerierState, QueryKind, RawQuery, Response, State,
    handle_query, post_query_params_body_first,
};

pub(crate) async fn query_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => {
            let resp = error.into_response();
            state.record_query("query_range", resp.status().is_success(), start);
            return resp;
        }
    };
    let resp = handle_query(state.clone(), headers, Some(&raw_query), QueryKind::Range).await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}
