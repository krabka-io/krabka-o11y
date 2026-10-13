use super::{
    Arc, BTreeMap, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RequestAuth, Response, State, StatusCode, tenant_ruler_rules, yaml_response,
};

pub(crate) async fn ruler_config_rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let snapshot = match tenant_ruler_rules(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
    ) {
        Ok(tenant_rules) => tenant_rules,
        Err(rejection) => return rejection.into_response(),
    };
    let mut rules = BTreeMap::new();
    for entry in snapshot.rules {
        rules.insert(entry.0, entry.1.into_values().collect::<Vec<_>>());
    }
    yaml_response(StatusCode::OK, &rules)
}
