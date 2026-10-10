use super::{
    ApiError, Arc, BTreeMap, DiscoveryLimits, DiscoveryParams, DiscoveryScope, IntoResponse,
    MetricStore, PrometheusApiState, RequestAuth, Response, discovery_scope,
    limit_discovery_results, success_data_response,
};

pub(crate) async fn labels_dispatch<S: MetricStore>(
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

    let mut names = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_names(tenant.as_str(), &matchers, start_ms, end_ms)
            .await
        {
            Ok(label_names) => {
                for name in label_names {
                    names.insert(name.clone(), name);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut names = names.into_values().collect::<Vec<_>>();
    if let Err(rejection) = limit_discovery_results(
        state,
        DiscoveryLimits {
            tenant: &tenant,
            limit: params.limit,
        },
        &mut names,
    ) {
        return rejection.into_response();
    }
    success_data_response(names)
}
