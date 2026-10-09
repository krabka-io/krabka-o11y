use super::{
    HeaderMap, HttpQueryError, Labels, QuerierState, RequestSecurity, SeriesParams,
    TenantErrorSurface, authorized_tenant, metadata_index_range, metadata_label_sets,
    metadata_time_range,
};

pub(crate) async fn series_data(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(&tenant);
    let tenant = tenant.as_str();
    let state = state
        .with_request_tenant_index_and_hot_range(
            tenant,
            metadata_index_range(state, params)?,
            metadata_time_range(params)?,
        )
        .await?;
    metadata_label_sets(&state, tenant, params).await
}
