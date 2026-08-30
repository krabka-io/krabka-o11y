use super::*;

pub(crate) async fn tail(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let params = match parse_query_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match prepare_http_tail(&state, &headers, &params).await {
        Ok(tail) => ws
            .on_upgrade(move |socket| send_tail_stream(socket, tail))
            .into_response(),
        Err(error) => error.into_response(),
    }
}
