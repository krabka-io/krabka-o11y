use super::{
    Arc, BTreeMap, Extension, HeaderMap, MetricStore, Principal, PrometheusApiState, Response,
    State, StatusCode, alerts::tenant_rule_handler, yaml_response,
};

tenant_rule_handler!(ruler_config_rules, state, snapshot, {
    let mut rules = BTreeMap::new();
    for entry in snapshot.rules {
        rules.insert(entry.0, entry.1.into_values().collect::<Vec<_>>());
    }
    yaml_response(StatusCode::OK, &rules)
});
