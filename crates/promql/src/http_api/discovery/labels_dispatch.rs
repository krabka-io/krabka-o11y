use super::{
    ApiError, Arc, BTreeMap, DiscoveryParams, MetricStore, PrometheusApiState, Rejection,
    RequestAuth, Response, discovery_response, discovery_scope, limit_discovery_results,
};

pub(crate) async fn labels_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    discovery_response(matched_label_names(state, auth, &params).await)
}

/// The distinct label names of the series that the request's selectors match.
async fn matched_label_names<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: &DiscoveryParams,
) -> Result<Vec<String>, Rejection> {
    let scope = discovery_scope(state, auth, params)?;
    let mut names = BTreeMap::new();
    for matchers in &scope.matcher_sets {
        let label_names = state
            .store
            .label_names(
                scope.tenant.as_str(),
                matchers,
                scope.start_ms,
                scope.end_ms,
            )
            .await
            .map_err(|error| Rejection::of(ApiError::from(error)))?;
        for name in label_names {
            names.insert(name.clone(), name);
        }
    }
    let mut names = names.into_values().collect::<Vec<_>>();
    limit_discovery_results(state, scope.limits(params), &mut names)?;
    Ok(names)
}
