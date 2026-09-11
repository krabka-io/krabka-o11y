use super::{
    HeaderMap, HttpQueryError, QuerierState, QueryKind, QueryParams, RequestSecurity,
    TenantErrorSurface, Value, authorized_tenants, execute_http_multi_tenant_query,
    execute_http_query_for_tenant, loki_stream_encoding_for_headers, parse_query,
};

pub(crate) async fn execute_http_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let tenants = authorized_tenants(
        state,
        security,
        headers,
        query_tenant_error_surface(&params, kind),
    )
    .await?;
    let encoding = loki_stream_encoding_for_headers(headers);
    if let [tenant] = tenants.as_slice() {
        return execute_http_query_for_tenant(state, tenant, &params, kind, encoding).await;
    }
    execute_http_multi_tenant_query(state, &tenants, &params, kind, encoding).await
}

// Loki 3.5.1 sends a range log query to its querier over gRPC, so a tenant
// error there comes back with the gRPC status prefix. A metric query answers
// 400 without it, and an instant log query answers 500. A query that is not a
// log query takes the metric answer. For a query that does not parse at all,
// Loki reports the parse error first. Krabka reports the tenant error, with the
// same 400.
fn query_tenant_error_surface(params: &QueryParams, kind: QueryKind) -> TenantErrorSurface {
    match (kind, parse_query(&params.query).is_ok()) {
        (_, false) => TenantErrorSurface::Read,
        (QueryKind::Range, true) => TenantErrorSurface::QuerierRead,
        (QueryKind::Instant, true) => TenantErrorSurface::InstantLogQuery,
    }
}
