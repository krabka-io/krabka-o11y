use serde_json::Value;

use super::{
    ApiError, Arc, BTreeMap, DiscoveryParams, MetricStore, PrometheusApiState, Rejection,
    RequestAuth, Response, discovery_response, discovery_scope, labels_json, labels_key,
    limit_discovery_results,
};

pub(crate) async fn series_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    discovery_response(matched_series(state, auth, &params).await)
}

/// The distinct label sets of the series that the request's selectors match,
/// as JSON objects.
async fn matched_series<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: &DiscoveryParams,
) -> Result<Vec<Value>, Rejection> {
    let scope = discovery_scope(state, auth, params)?;
    let mut by_key = BTreeMap::new();
    for matchers in &scope.matcher_sets {
        let series = state
            .store
            .series(
                scope.tenant.as_str(),
                matchers,
                scope.start_ms,
                scope.end_ms,
            )
            .await
            .map_err(|error| Rejection::of(ApiError::from(error)))?;
        for labels in series {
            by_key.insert(labels_key(&labels), labels);
        }
    }
    let mut series = by_key
        .into_values()
        .map(|labels| labels_json(labels.iter()))
        .collect::<Vec<_>>();
    limit_discovery_results(state, scope.limits(params), &mut series)?;
    Ok(series)
}
