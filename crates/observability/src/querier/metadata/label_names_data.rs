use super::*;

pub(crate) async fn label_names_data(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    let mut names = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        names.extend(labels.keys().cloned());
    }

    Ok(names.into_iter().collect::<Vec<_>>())
}
