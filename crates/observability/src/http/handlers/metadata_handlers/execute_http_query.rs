use super::{
    HeaderMap, HttpQueryError, QuerierState, QueryKind, QueryParams, Value, authorized_tenants,
    execute_http_multi_tenant_query, execute_http_query_for_tenant,
};

pub(crate) async fn execute_http_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let tenants = authorized_tenants(state, headers).await?;
    if tenants.len() > 1 {
        return execute_http_multi_tenant_query(state, &tenants, &params, kind).await;
    }
    execute_http_query_for_tenant(state, &tenants[0], &params, kind).await
}
