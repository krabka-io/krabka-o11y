use super::{
    HeaderMap, HttpQueryError, IntoResponse, QuerierState, QueryKind, RequestSecurity, Response,
    TenantErrorSurface, api_prom_streams_only_response, execute_http_query, parse_query_params,
};

pub(crate) async fn handle_api_prom_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &security, &headers, params, QueryKind::Instant).await {
        Ok(value) => api_prom_streams_only_response(&value),
        // The legacy endpoint reaches Loki's querier over gRPC for every query
        // it accepts, so a tenant error keeps the gRPC status prefix.
        Err(HttpQueryError::Tenant { source, .. }) => HttpQueryError::Tenant {
            source,
            surface: TenantErrorSurface::QuerierRead,
        }
        .into_response(),
        Err(error) => error.into_response(),
    }
}
