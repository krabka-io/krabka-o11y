use super::*;

pub(crate) async fn series_data(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    metadata_label_sets(&state, tenant, params).await
}
