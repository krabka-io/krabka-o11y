use super::{BTreeMap, LokiRuleNamespaces};

pub(crate) fn loki_rule_namespace_response(
    namespaces: &LokiRuleNamespaces,
) -> BTreeMap<String, Vec<serde_yaml::Value>> {
    let mut response = BTreeMap::new();
    for namespace in namespaces {
        response.insert(namespace.0.clone(), namespace.1.values().cloned().collect());
    }
    response
}
