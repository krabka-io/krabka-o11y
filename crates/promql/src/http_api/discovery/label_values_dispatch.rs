use super::{
    ApiError, Arc, BTreeMap, DiscoveryLimits, DiscoveryScope, IntoResponse, LabelValuesQuery,
    MetricStore, PrometheusApiState, RequestAuth, Response, discovery_scope,
    limit_discovery_results, success_data_response,
};

pub(crate) async fn label_values_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    query: LabelValuesQuery,
) -> Response {
    let LabelValuesQuery { name, params } = query;
    let DiscoveryScope {
        tenant,
        start_ms,
        end_ms,
        matcher_sets,
    } = match discovery_scope(state, auth, &params) {
        Ok(scope) => scope,
        Err(rejection) => return rejection.into_response(),
    };

    let mut values = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_values(tenant.as_str(), &name, &matchers, start_ms, end_ms)
            .await
        {
            Ok(label_values) => {
                for value in label_values {
                    values.insert(value.clone(), value);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut values = values.into_values().collect::<Vec<_>>();
    if let Err(rejection) = limit_discovery_results(
        state,
        DiscoveryLimits {
            tenant: &tenant,
            limit: params.limit,
        },
        &mut values,
    ) {
        return rejection.into_response();
    }
    success_data_response(
        values
            .into_iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>(),
    )
}
