use super::{
    ApiError, Arc, BTreeMap, LabelValuesQuery, MetricStore, PrometheusApiState, Rejection,
    RequestAuth, Response, discovery_response, discovery_scope, limit_discovery_results,
};

pub(crate) async fn label_values_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    query: LabelValuesQuery,
) -> Response {
    discovery_response(matched_label_values(state, auth, &query).await)
}

/// The distinct values of one label across the series that the request's
/// selectors match.
async fn matched_label_values<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    query: &LabelValuesQuery,
) -> Result<Vec<String>, Rejection> {
    let LabelValuesQuery { name, params } = query;
    let scope = discovery_scope(state, auth, params)?;
    let mut values = BTreeMap::new();
    for matchers in &scope.matcher_sets {
        let label_values = state
            .store
            .label_values(
                scope.tenant.as_str(),
                name,
                matchers,
                scope.start_ms,
                scope.end_ms,
            )
            .await
            .map_err(|error| Rejection::of(ApiError::from(error)))?;
        for value in label_values {
            values.insert(value.clone(), value);
        }
    }
    let mut values = values.into_values().collect::<Vec<_>>();
    limit_discovery_results(state, scope.limits(params), &mut values)?;
    Ok(values
        .into_iter()
        .map(|value| value.as_str().to_owned())
        .collect())
}
