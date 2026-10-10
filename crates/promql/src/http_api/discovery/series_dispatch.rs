use super::{
    ApiError, Arc, BTreeMap, DiscoveryLimits, DiscoveryParams, DiscoveryScope, IntoResponse,
    MetricStore, PrometheusApiState, RequestAuth, Response, discovery_scope, labels_json,
    labels_key, limit_discovery_results, success_data_response,
};

pub(crate) async fn series_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    let DiscoveryScope {
        tenant,
        start_ms,
        end_ms,
        matcher_sets,
    } = match discovery_scope(state, auth, &params) {
        Ok(scope) => scope,
        Err(rejection) => return rejection.into_response(),
    };

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .series(tenant.as_str(), &matchers, start_ms, end_ms)
            .await
        {
            Ok(series) => {
                for labels in series {
                    by_key.insert(labels_key(&labels), labels);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut series = by_key
        .into_values()
        .map(|labels| labels_json(labels.iter()))
        .collect::<Vec<_>>();
    if let Err(rejection) = limit_discovery_results(
        state,
        DiscoveryLimits {
            tenant: &tenant,
            limit: params.limit,
        },
        &mut series,
    ) {
        return rejection.into_response();
    }
    success_data_response(series)
}
