use super::{
    HeaderMap, IntoResponse, QuerierState, QueryKind, RequestSecurity, Response, StatusCode,
    add_loki_encoding_flags, execute_http_query, json_response, loki_encoding_flags,
    loki_parquet_response, parse_query_params, wants_loki_parquet,
};

pub(crate) async fn handle_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
    kind: QueryKind,
) -> Response {
    let wants_parquet = wants_loki_parquet(&headers);
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    let flags = loki_encoding_flags(&headers);
    match execute_http_query(&state, &security, &headers, params, kind).await {
        Ok(value) if wants_parquet => match loki_parquet_response(&value) {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Ok(mut value) => {
            add_loki_encoding_flags(&mut value, &flags);
            json_response(StatusCode::OK, &value)
        }
        Err(error) => error.into_response(),
    }
}
