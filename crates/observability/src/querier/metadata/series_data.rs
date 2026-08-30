use super::{
    HeaderMap, HttpQueryError, Labels, QuerierState, SeriesParams, authorized_tenant,
    metadata_index_range, metadata_label_sets,
};

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
