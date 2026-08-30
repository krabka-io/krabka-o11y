use super::*;

pub(crate) async fn cardinality_series_for_params<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    let selector = params.selector.as_deref().unwrap_or_default();
    let matcher_sets = selector_matchers(selector).map_err(ApiError::from)?;
    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        let series = state
            .store
            .series(tenant, &matchers, i64::MIN, i64::MAX)
            .await
            .map_err(ApiError::from)?;
        for labels in series {
            by_key.insert(labels_key(&labels), labels);
        }
    }
    Ok(by_key.into_values().collect())
}
