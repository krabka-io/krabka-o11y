use super::{
    HeaderMap, IntoResponse, QuerierState, RawQuery, RequestSecurity, Response, State,
    WebSocketUpgrade, parse_query_params, prepare_http_tail, send_tail_stream,
};

pub(crate) async fn tail(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let params = match parse_query_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match prepare_http_tail(&state, &security, &headers, &params).await {
        Ok(tail) => ws
            .on_upgrade(move |socket| send_tail_stream(socket, tail))
            .into_response(),
        Err(error) => error.into_response(),
    }
}
