use super::{
    HeaderMap, HttpQueryError, QuerierState, QueryKind, RequestSecurity, Response,
    TenantErrorSurface, api_prom_streams_only_response, execute_http_query, parse_query_params,
};

pub(crate) async fn handle_api_prom_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Result<Response, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    match execute_http_query(&state, &security, &headers, params, QueryKind::Instant).await {
        Ok(value) => Ok(api_prom_streams_only_response(&value)),
        // The legacy endpoint reaches Loki's querier over gRPC for every query
        // it accepts, so a tenant error keeps the gRPC status prefix.
        Err(HttpQueryError::Tenant { source, .. }) => Err(HttpQueryError::Tenant {
            source,
            surface: TenantErrorSurface::QuerierRead,
        }),
        Err(error) => Err(error),
    }
}
