use super::*;

pub(crate) async fn handle_query(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
    kind: QueryKind,
) -> Response {
    let wants_parquet = wants_loki_parquet(&headers);
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, kind).await {
        Ok(value) if wants_parquet => match loki_parquet_response(&value) {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}
