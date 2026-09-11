use super::{
    BTreeSet, HeaderMap, HttpQueryError, QuerierState, RequestSecurity, SeriesParams,
    TenantErrorSurface, authorized_tenant, metadata_index_range, metadata_label_sets,
};

pub(crate) async fn label_names_data(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(&tenant);
    let tenant = tenant.as_str();
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(state, params)?)
        .await?;
    let mut names = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        names.extend(labels.keys().cloned());
    }

    Ok(names.into_iter().collect::<Vec<_>>())
}
