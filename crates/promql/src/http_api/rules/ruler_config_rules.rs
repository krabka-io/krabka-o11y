use super::{
    Arc, BTreeMap, Extension, HeaderMap, MetricStore, Principal, PrometheusApiState, Response,
    State, StatusCode, alerts::tenant_rules_or_return, yaml_response,
};

pub(crate) async fn ruler_config_rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let snapshot = tenant_rules_or_return!(&state, &headers, &principal);
    let mut rules = BTreeMap::new();
    for entry in snapshot.rules {
        rules.insert(entry.0, entry.1.into_values().collect::<Vec<_>>());
    }
    yaml_response(StatusCode::OK, &rules)
}
