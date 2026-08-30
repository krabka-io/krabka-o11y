use super::{HeaderMap, HttpQueryError, QuerierState, tenant};

pub(crate) async fn authorized_tenants(
    state: &QuerierState,
    headers: &HeaderMap,
) -> Result<Vec<String>, HttpQueryError> {
    let header = tenant(headers)?;
    let tenants = header
        .split('|')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tenants.iter().any(String::is_empty) {
        return Err(HttpQueryError::InvalidTenant);
    }
    for tenant in &tenants {
        state.query_authorizer.check(tenant).await?;
    }
    Ok(tenants)
}
