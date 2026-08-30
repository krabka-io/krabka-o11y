use super::*;

pub(crate) async fn authorized_tenant<'a>(
    state: &QuerierState,
    headers: &'a HeaderMap,
) -> Result<&'a str, HttpQueryError> {
    let tenant = tenant(headers)?;
    state.query_authorizer.check(tenant).await?;
    Ok(tenant)
}
