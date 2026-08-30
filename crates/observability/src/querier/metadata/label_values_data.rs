use super::*;

pub(crate) async fn label_values_data(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    let mut values = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        if let Some(value) = labels.get(name) {
            values.insert(value.clone());
        }
    }

    Ok(values.into_iter().collect::<Vec<_>>())
}
